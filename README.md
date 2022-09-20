# Kinesis Connector for Spark Structured Streaming 

Implementation of Kinesis Source Provider in Spark Structured Streaming. [SPARK-18165](https://issues.apache.org/jira/browse/SPARK-18165) describes the need for such implementation. More details on the implementation can be read in this [blog](https://www.qubole.com/blog/kinesis-connector-for-structured-streaming/)

This is a fork of https://github.com/qubole/kinesis-sql with the build configuration and source code updated for building against Spark 3.2.1 in order to fix a number of bugs involving the consumer not receiving new messages after a period of no new messages being added to the Kinesis data stream.

## Downloading and Using the Connector

The connector is available from the Maven Central repository. It can be used using the --packages option or the spark.jars.packages configuration property. Use the following connector artifact

	Spark 3.2: com.roncemer.spark/spark-sql-kinesis_2.13/1.2.1-spark_3.2

## Developer Setup
Clone spark-sql-kinesis from the source repository on GitHub.

###### Spark version 3.2.x
	git clone git@github.com:roncemer/spark-sql-kinesis.git
	git checkout master
	cd spark-sql-kinesis
	mvn install -DskipTests

This will create *target/spark-sql-kinesis_2.13-1.2.1_spark-3.2.jar* file which contains the connector code and its dependency jars.


## How to use it

#### Setup Kinesis
Refer [Amazon Docs](https://docs.aws.amazon.com/cli/latest/reference/kinesis/create-stream.html) for more options

###### Create Kinesis Stream 

	$ aws kinesis create-stream --stream-name test --shard-count 2
    
###### Add Records in the stream
	
    $ aws kinesis put-record --stream-name test --partition-key 1 --data 'Kinesis'
    $ aws kinesis put-record --stream-name test --partition-key 1 --data 'Connector'
    $ aws kinesis put-record --stream-name test --partition-key 1 --data 'for'
    $ aws kinesis put-record --stream-name test --partition-key 1 --data 'Apache'
    $ aws kinesis put-record --stream-name test --partition-key 1 --data 'Spark'

#### Example Streaming Job

Refering $SPARK_HOME to the Spark installation directory.

###### Open Spark-Shell

	$SPARK_HOME/bin/spark-shell --jars target/spark-sql-kinesis_2.13-1.2.1_spark-3.2.jar

###### Subscribe to Kinesis Source
	// Subscribe the "test" stream
	scala> :paste
	val kinesis = spark
  		.readStream
  		.format("kinesis")
    	.option("streamName", "spark-streaming-example")
       	.option("endpointUrl", "https://kinesis.us-east-1.amazonaws.com")
        .option("awsAccessKeyId", [ACCESS_KEY])
        .option("awsSecretKey", [SECRET_KEY])
        .option("startingposition", "TRIM_HORIZON")
  		.load

###### Check Schema 
	scala> kinesis.printSchema
	root
 	|-- data: binary (nullable = true)
 	|-- streamName: string (nullable = true)
 	|-- partitionKey: string (nullable = true)
 	|-- sequenceNumber: string (nullable = true)
 	|-- approximateArrivalTimestamp: timestamp (nullable = true)

###### Word Count 
	// Cast data into string and group by data column
	scala> :paste
    
    	 kinesis
        .selectExpr("CAST(data AS STRING)").as[(String)]
        .groupBy("data").count()
  		.writeStream
  		.format("console")
        .outputMode("complete") 
  		.start()
  		.awaitTermination()
        
###### Output in Console


	+------------+-----+
	|        data|count|
	+------------+-----+
	|         for|    1|
	|      Apache|    1|
    |       Spark|    1|
	|     Kinesis|    1|
	|   Connector|    1|
	+------------+-----+ 

###### Using the Kinesis Sink
    // Cast data into string and group by data column
        scala> :paste
        kinesis
        .selectExpr("CAST(rand() AS STRING) as partitionKey","CAST(data AS STRING)").as[(String,String)]
        .groupBy("data").count()
  	    .writeStream
  	    .format("kinesis")
        .outputMode("update") 
        .option("streamName", "spark-sink-example")
        .option("endpointUrl", "https://kinesis.us-east-1.amazonaws.com")
        .option("awsAccessKeyId", [ACCESS_KEY])
        .option("awsSecretKey", [SECRET_KEY])
  	    .start()
  	    .awaitTermination()

## Kinesis Source Configuration 

 Option-Name        | Default-Value           | Description  |
| ------------- |:-------------:| -----:|
| streamName     | - | Name of the stream in Kinesis to read from |
| endpointUrl     |   https://kinesis.us-east-1.amazonaws.com    |   end-point URL for Kinesis Stream|
| awsAccessKeyId |    -     |    AWS Credentials for Kinesis describe, read record operations |
| awsSecretKey |      -  |    AWS Credentials for Kinesis describe, read record operations |
| awsSTSRoleARN |      -  |    AWS STS Role ARN for Kinesis describe, read record operations |
| awsSTSSessionName |      -  |    AWS STS Session name for Kinesis describe, read record operations |
| awsUseInstanceProfile | true |    Use Instance Profile Credentials if none of credentials provided |
| startingPosition |      LATEST |    Starting Position in Kinesis to fetch data from. Possible values are "latest", "trim_horizon", "earliest" (alias for trim_horizon), or JSON serialized map shardId->KinesisPosition   |
| failondataloss| true | fail the streaming job if any active shard is missing or expired
| kinesis.executor.maxFetchTimeInMs |     1000 |  Maximum time spent in executor to fetch record from Kinesis per Shard |
| kinesis.executor.maxFetchRecordsPerShard |     100000 |  Maximum Number of records to fetch per shard  |
| kinesis.executor.maxRecordPerRead |     10000 |  Maximum Number of records to fetch per getRecords API call  |
| kinesis.executor.addIdleTimeBetweenReads	| false	| Add delay between two consecutive getRecords API call	|
| kinesis.executor.idleTimeBetweenReadsInMs	| 1000	| Minimum delay between two consecutive getRecords	| 
| kinesis.client.describeShardInterval |      1s (1 second) |  Minimum Interval between two ListShards API calls to consider resharding  |
| kinesis.client.numRetries |     3 |  Maximum Number of retries for Kinesis API requests  |
| kinesis.client.retryIntervalMs |     1000 |  Cool-off period before retrying Kinesis API  |
| kinesis.client.maxRetryIntervalMs	| 10000	| Max Cool-off period between 2 retries	|
| kinesis.client.avoidEmptyBatches| false | Avoid creating an empty microbatch job by checking upfront if there are any unread data in the stream before the batch is started

## Kinesis Sink Configuration
 Option-Name        | Default-Value           | Description  |
| ------------- |:-------------:| -----:|
| streamName   | - | Name of the stream in Kinesis to write to|
| endpointUrl  | https://kinesis.us-east-1.amazonaws.com |  The aws endpoint of the kinesis Stream |
| awsAccessKeyId |    -     |    AWS Credentials for  Kinesis describe, read record operations    
| awsSecretKey |      -  |    AWS Credentials for  Kinesis describe, read record |
| awsSTSRoleARN |      -  |    AWS STS Role ARN for Kinesis describe, read record operations |
| awsSTSSessionName |      -  |    AWS STS Session name for Kinesis describe, read record operations |
| awsUseInstanceProfile | true |    Use Instance Profile Credentials if none of credentials provided |
| kinesis.executor.recordMaxBufferedTime | 1000 (millis) | Specify the maximum buffered time of a record |
| kinesis.executor.maxConnections | 1 | Specify the maximum connections to Kinesis | 
| kinesis.executor.aggregationEnabled | true | Specify if records should be aggregated before sending them to Kinesis |
| kniesis.executor.flushwaittimemillis | 100 | Wait time while flushing records to Kinesis on Task End |

## Roadmap
*  We need to migrate to DataSource V2 APIs for MicroBatchExecution.
*  Maintain Per Micro-Batch Shard Commit state in Dynamo DB

## Acknowledgement

This connector would not have been possible without reference implemetation of [Kafka connector](https://github.com/apache/spark/tree/branch-2.2/external/kafka-0-10-sql) for Structured streaming, [Kinesis Connector](https://github.com/apache/spark/tree/branch-2.2/external/kinesis-asl) for Legacy Streaming and [Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client). Structure of some part of the code is influenced by the excellent work done by various Apache Spark Contributors.
