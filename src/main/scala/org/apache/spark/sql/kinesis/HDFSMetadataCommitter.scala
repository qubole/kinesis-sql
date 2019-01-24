/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kinesis

import java.io.{FileNotFoundException, InputStream, InputStreamReader, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.{EnumSet, Locale}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.util.SerializableConfiguration


/*
   [[HDFSFileCommitter]] is used by executors to commit metadata to a HDFS location
   It is similar to [[HDFSMetadataLog]]. Difference is that it does not use
   [[SparkSession]] while creating fileContext. Hence it can used by executors.
   We could have modified [[HDFSMetadataLog]] but then changes for kinesis support
   would not have been contained within an external jar
 */

class HDFSMetadataCommitter[T <: AnyRef : ClassTag](path: String,
  hadoopConf: SerializableConfiguration,
  options: Map[String, String] = Map.empty[String, String])
  extends MetadataCommitter[T] with Logging with Serializable{


  private implicit val formats = Serialization.formats(NoTypeHints)

  /** Needed to serialize type T into JSON when using Jackson */
  private implicit val manifest = Manifest.classType[T](implicitly[ClassTag[T]].runtimeClass)

  val metadataPath = new Path(path, "shard-commit")

  protected val fileContext = FileContext.getFileContext(
    metadataPath.toUri, hadoopConf.value.asInstanceOf[ Configuration ])

  if ( !fileContext.util().exists(metadataPath) ) {
    fileContext.mkdir(metadataPath, FsPermission.getDirDefault, true)
  }

  private val numRetries: Int = {
    options.getOrElse("executor.metadata.hdfs.numretries", "3").toInt
  }

  private val retryIntervalMs: Long = {
    options.getOrElse("executor.metadata.hdfs.retryIntervalMs".toLowerCase(Locale.ROOT),
      "1000").toLong
  }

  private val maxRetryIntervalMs: Long = {
    options.getOrElse("executor.metadata.hdfs.maxRetryIntervalMs".toLowerCase(Locale.ROOT),
      "10000").toLong
  }


  /*
   *  A `PathFilter` to filter only batch files
   */

  protected val batchFilesFilter = new PathFilter {
    override def accept(path: Path): Boolean = isBatchFile(path)
  }

  protected def batchIdToPath(batchId: Long): Path = {
    new Path(metadataPath, batchId.toString)
  }

  protected def pathToBatchId(path: Path) = {
    path.getName.toLong
  }

  protected def isBatchFile(path: Path) = {
    try {
      path.getName.toLong
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  protected def serialize(metadata: T, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    Serialization.write(metadata, out)
  }

  protected def deserialize(in: InputStream): T = {
    // called inside a try-finally where the underlying stream is closed in the caller
    val reader = new InputStreamReader(in, StandardCharsets.UTF_8)
    Serialization.read[T](reader)
  }

  def create(batchId: Long): Unit = {
    val newPath = batchIdToPath(batchId)
    if ( !fileContext.util().exists(newPath) ) {
      fileContext.mkdir(newPath, FsPermission.getDirDefault, true)
    }
  }

  override def add(batchId: Long, shardId: String, metadata: T): Boolean = {
    require(metadata != null, "'null' metadata cannot written to a shard commit log")
    create(batchId)
    val shardCommitPath = new Path(batchIdToPath(batchId), shardId)
    import CreateFlag._
    import Options._

    val output = fileContext.create(shardCommitPath,
      EnumSet.of(CREATE, OVERWRITE), CreateOpts.checksumParam(ChecksumOpt.createDisabled()))
    try {
      serialize(metadata, output)
      output.close()
    } catch {
      case e: Throwable =>
        // close the open stream and delete the new file added
        output.close()
        withRetry[Boolean]("deleting cancelled metadataFile") {
          fileContext.delete(shardCommitPath, false)
        }
        // throw the exception again so that the caller knows that add operation was not successful
        throw e
    }
    true
  }

  override def get(batchId: Long): Seq[T] = {
    val batchMetadataDir = batchIdToPath(batchId)
    withRetry[ Seq[ T ] ]("fetching MetaData") {
      if ( fileContext.util().exists(batchMetadataDir) ) {
        fileContext.util().listStatus(batchMetadataDir).map { f =>
          getData(f.getPath) match {
            case Some(data) => data
            case None =>
              // return if there is any one filepath from which we could not read any data
              logDebug(s"Unable to get data for ${f.getPath}")
              throw new IllegalStateException(s"Failed to get metadata for ${f.getPath}")
          }
        }.toSeq
      } else {
        logDebug(s"Unable to find batch $batchMetadataDir")
        throw new IllegalStateException(s"$batchMetadataDir does not exist")
      }
    }
  }

  def getData(path: Path): Option[ T ] = {
      if ( fileContext.util().exists(path) ) {
        val input = fileContext.open(path)
        try {
          Some(deserialize(input))
        } catch {
          case ise: IllegalStateException => // re-throw the exception with the log file path added
            throw new IllegalStateException(s"Failed to read log file ${path}. " +
              s"${ise.getMessage}", ise)
        } finally {
          IOUtils.closeQuietly(input)
        }
      } else {
        logDebug(s"Unable to find file $path")
        None
      }
    }

  def delete(batchId: Long): Unit = {
    val batchMetadataDir = batchIdToPath(batchId)
    delete(batchMetadataDir)
  }

  def delete(path: Path): Unit = {
    try {
      fileContext.delete(path, true)
    } catch {
      case e: FileNotFoundException => // ignore if file has already been deleted
    }
  }

  /*
   * Removes all the log entry earlier than thresholdBatchId (exclusive).
   */
  override def purge(thresholdBatchId: Long): Unit = {
    val batchIds = fileContext.util().listStatus(metadataPath, batchFilesFilter)
        .map(f => pathToBatchId(f.getPath))

    for (batchId <- batchIds if batchId < thresholdBatchId) {
      val path = batchIdToPath(batchId)
      delete(path)
      logTrace(s"Removed metadata log file: $path")
    }
  }

  /** Helper method to retry with exponential backoff  */
  def withRetry[ T ](message: String, ignoreException: Boolean = true)(body: => T): T = {
    var retryCount = 0
    var result: Option[ T ] = None
    var waitTimeInterval = retryIntervalMs
    var lastError: Throwable = null

    def isMaxRetryDone = retryCount >= numRetries

    while (result.isEmpty && !isMaxRetryDone) {
      if ( retryCount > 0 ) { // wait only if this is a retry
        Thread.sleep(waitTimeInterval)
        waitTimeInterval = scala.math.min(waitTimeInterval * 2, maxRetryIntervalMs)
      }
      try {
        result = Some(body)
      } catch {
        case NonFatal(t) => lastError = t
          if ( ignoreException ) {
            logWarning(s"Error while $message [attempt = ${retryCount + 1}]", t)
          } else {
            throw new IllegalStateException(s"Error while $message", t)
          }
      }
      retryCount += 1
    }
    result.getOrElse {
      throw new IllegalStateException(s"Gave up after $retryCount retries while $message," +
          s" last exception: ", lastError)
    }
  }
}
