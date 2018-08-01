# Kinesis Connector for Structured Streaming 

Implementation of Kinesis Source Provider in Spark Structured Streaming. [SPARK-18165](https://issues.apache.org/jira/browse/SPARK-18165) describes the need for such implementation. 

## Developer Setup
Checkout kinesis-sql branch depending upon your Spark version. Use Master branch for the latest Spark version 

###### Spark version 2.2.0
	git clone git@github.com:qubole/kinesis-sql.git
	git checkout 2.2.0
	cd kinesis-sql
	mvn install -DskipTests

This will create *target/spark-sql-kinesis_2.11-2.2.0.jar* file which contains the connector code and its dependency jars.

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

Refering $SPARK_HOME to the Spark installation directory. This library has been developed and tested against **SPARK 2.2.x**. 

###### Open Spark-Shell

	$SPARK_HOME/bin/spark-shell --jars target/spark-sql-kinesis_2.11-2.2.0.jar

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
| awsAccessKeyId |    -     |    AWS Credentials for  Kinesis describe, read record operations    
| awsSecretKey |      -  |    AWS Credentials for  Kinesis describe, read record |
| startingPosition |      LATEST |    Starting Position in Kinesis to fetch data from. Possible values are "LATEST" & "TRIM_HORIZON" |
| describeShardInterval |      1s (1 second) |  Minimum Interval between two DescribeStream API calls to consider resharding  |
| kinesis.executor.maxFetchTimeInMs |     1000 |  Maximum time spent in executor to fetch record from Kinesis per Shard |
| kinesis.executor.maxFetchRecordsPerShard |     100000 |  Maximum Number of records to fetch per shard  |
| kinesis.executor.maxRecordPerRead |     10000 |  Maximum Number of records to fetch per getRecords API call  |
| kinesis.client.numRetries |     3 |  Maximum Number of retries for Kinesis API requests  |
| kinesis.client.retryIntervalMs |     1000 |  Cool-off period before retrying Kinesis API  |

## Kinesis Sink Configuration
 Option-Name        | Default-Value           | Description  |
| ------------- |:-------------:| -----:|
| streamName   | - | Name of the stream in Kinesis to write to|
| endpointUrl  | https://kinesis.us-east-1.amazonaws.com |  The aws endpoint of the kinesis Stream |
| awsAccessKeyId |    -     |    AWS Credentials for  Kinesis describe, read record operations    
| awsSecretKey |      -  |    AWS Credentials for  Kinesis describe, read record |
| kinesis.executor.recordMaxBufferedTime | 1000 (millis) | Specify the maximum buffered time of a record |
| kinesis.executor.maxConnections | 1 | Specify the maximum connections to Kinesis | 
| kinesis.executor.aggregationEnabled | true | Specify if records should be aggregated before sending them to Kinesis | 

## Roadmap
*  Above library has been developed and tested against Spark 2.2.x.  We need to migrate to DataSource V2 APIs released in Spark 2.3.0

## Acknowledgement

This connector would not have been possible without reference implemetation of [Kafka connector](https://github.com/apache/spark/tree/branch-2.2/external/kafka-0-10-sql) for Structured streaming, [Kinesis Connector](https://github.com/apache/spark/tree/branch-2.2/external/kinesis-asl) for Legacy Streaming and [Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client). Structure of some part of the code is influenced by the excellent work done by various Apache Spark Contributors.
