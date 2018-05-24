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
import java.util.EnumSet

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CreateFlag, FileContext, Path, PathFilter}
import org.apache.hadoop.fs.permission.FsPermission
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import scala.reflect.ClassTag

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
  hadoopConf: SerializableConfiguration)
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
     if (fileContext.util().exists(shardCommitPath)) {
      delete(shardCommitPath)
    }
    val output = fileContext.create(shardCommitPath, EnumSet.of(CreateFlag.CREATE))
    serialize(metadata, output)
    return true
  }

  override def get(batchId: Long): Seq[T] = {
    val batchMetadataDir = batchIdToPath(batchId)
    if ( fileContext.util().exists(batchMetadataDir) ) {
      fileContext.util().listStatus(batchMetadataDir).map { f =>
        val input = fileContext.open(f.getPath)
        val shardPosition = try {
          deserialize(input)
        } catch {
          case ise: IllegalStateException => // re-throw the exception with the log file path added
            throw new IllegalStateException(
              s"Failed to read log file ${f.getPath}. ${ise.getMessage}", ise)
        } finally {
          IOUtils.closeQuietly(input)
          None
        }
        shardPosition
      }.toSeq
    } else {
      logDebug(s"Unable to find batch $batchMetadataDir")
      null
    }
  }

  def delete(batchId: Long): Unit = {
    val batchMetadataDir = batchIdToPath(batchId)
    delete(batchId)
  }

  def delete(path: Path): Unit = {
    try {
      fileContext.delete(path, true)
    } catch {
      case e: FileNotFoundException =>
      // ignore if file has already been deleted
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

}
