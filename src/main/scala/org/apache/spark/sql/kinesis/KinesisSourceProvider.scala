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

import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/*
 * The provider class for the [[KinesisSource]]. This provider is designed such that it throws
 * IllegalArgumentException when the Kinesis Dataset is created, so that it can catch
 * missing options even before the query is started.
 */

private[kinesis] class KinesisSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with StreamSinkProvider
  with Logging {

  import KinesisSourceProvider._

  override def shortName(): String = "kinesis"

  /*
   *  Returns the name and schema of the source. In addition, it also verifies whether the options
   * are correct and sufficient to create the [[KinesisSource]] when the query is started.
   */

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    validateStreamOptions(caseInsensitiveParams)
    require(schema.isEmpty, "Kinesis source has a fixed schema and cannot be set with a custom one")
    (shortName(), KinesisSource.kinesisSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    validateStreamOptions(caseInsensitiveParams)

    val specifiedKinesisParams =
      parameters
        .keySet
        .filter(_.toLowerCase(Locale.ROOT).startsWith("kinesis."))
        .map { k => k.drop(8).toString -> parameters(k) }
        .toMap

    val streamName = caseInsensitiveParams.get(STREAM_NAME_KEY).get

    val awsAccessKeyId = caseInsensitiveParams.get(AWS_ACCESS_KEY_ID).getOrElse("")
    val awsSecretKey = caseInsensitiveParams.get(AWS_SECRET_KEY).getOrElse("")

    val regionName = caseInsensitiveParams.get(REGION_NAME_KEY)
      .getOrElse(DEFAULT_KINESIS_REGION_NAME)
    val endPointURL = caseInsensitiveParams.get(END_POINT_URL)
      .getOrElse(DEFAULT_KINESIS_ENDPOINT_URL)

    val initialPosition: KinesisPosition = getKinesisPosition(caseInsensitiveParams)

    val kinesisCredsProvider: BasicCredentials = BasicCredentials(awsAccessKeyId, awsSecretKey)

    new KinesisSource(
      sqlContext, specifiedKinesisParams, metadataPath,
      streamName, initialPosition, endPointURL, kinesisCredsProvider)
  }

  private def validateStreamOptions(caseInsensitiveParams: Map[String, String]) = {
    if (!caseInsensitiveParams.contains(STREAM_NAME_KEY) ||
      caseInsensitiveParams.get(STREAM_NAME_KEY).get.isEmpty) {
      throw new IllegalArgumentException(
        "Stream name is a required field")
    }
  }

  private def validateSinkOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    if (!caseInsensitiveParams.contains(SINK_STREAM_NAME_KEY) ||
      caseInsensitiveParams(SINK_STREAM_NAME_KEY).isEmpty) {
      throw new IllegalArgumentException(
        "Stream name is a required field")
    }
    if (!caseInsensitiveParams.contains(SINK_ENDPOINT_URL) ||
      caseInsensitiveParams(SINK_ENDPOINT_URL).isEmpty) {
      throw new IllegalArgumentException(
        "Sink endpoint url is a required field")
    }
    if (caseInsensitiveParams.contains(SINK_AGGREGATION_ENABLED) && (
        caseInsensitiveParams(SINK_AGGREGATION_ENABLED).trim != "true" &&
        caseInsensitiveParams(SINK_AGGREGATION_ENABLED).trim != "false"
      )) {
      throw new IllegalArgumentException(
        "Sink aggregation value must be either true or false")
    }
  }

  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): Sink = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    validateSinkOptions(caseInsensitiveParams)
    new KinesisSink(sqlContext, caseInsensitiveParams, outputMode)
  }

}

private[kinesis] object KinesisSourceProvider extends Logging {

  private[kinesis] val STREAM_NAME_KEY = "streamname"
  private[kinesis] val END_POINT_URL = "endpointurl"
  private[kinesis] val REGION_NAME_KEY = "regionname"
  private[kinesis] val AWS_ACCESS_KEY_ID = "awsaccesskeyid"
  private[kinesis] val AWS_SECRET_KEY = "awssecretkey"
  private[kinesis] val STARTING_POSITION_KEY = "startingposition"

  // Sink Options
  private[kinesis] val SINK_STREAM_NAME_KEY = "streamname"
  private[kinesis] val SINK_ENDPOINT_URL = "endpointurl"
  private[kinesis] val SINK_RECORD_MAX_BUFFERED_TIME = "kinesis.executor.recordmaxbufferedtime"
  private[kinesis] val SINK_MAX_CONNECTIONS = "kinesis.executor.maxconnections"
  private[kinesis] val SINK_AGGREGATION_ENABLED = "kinesis.executor.aggregationenabled"


  private[kinesis] def getKinesisPosition(
      params: Map[String, String]): KinesisPosition = {
    // TODO Support custom shards positions
    val CURRENT_TIMESTAMP = System.currentTimeMillis
    params.get(STARTING_POSITION_KEY).map(_.trim) match {
      case Some(position) if position.toLowerCase(Locale.ROOT) == "latest" =>
        new AtTimeStamp(CURRENT_TIMESTAMP)
      case Some(position) if position.toLowerCase(Locale.ROOT) == "trim_horizon" =>
        new TrimHorizon
      case None => new AtTimeStamp(CURRENT_TIMESTAMP)
    }
  }

  private[kinesis] val DEFAULT_KINESIS_ENDPOINT_URL: String =
    "https://kinesis.us-east-1.amazonaws.com"

  private[kinesis] val DEFAULT_KINESIS_REGION_NAME: String = "us-east-1"

  private[kinesis] val DEFAULT_SINK_RECORD_MAX_BUFFERED_TIME: String = "1000"

  private[kinesis] val DEFAULT_SINK_MAX_CONNECTIONS: String = "1"

  private[kinesis] val DEFAULT_SINK_AGGREGATION: String = "true"
}



