from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from pyspark.sql import DataFrame
import time
import logging
from datetime import datetime


#Variable declaration

#cassandra database detail
KEYSPACE = "fraud"
TABLE="fraud_detection"

#kafka server detail
KAFKA_BOOTSTRAP_SERVER="kafka:9092"
KAFKA_TOPIC = "fraud"

#Cassandra database connectivity credentails
CASSANDRA_HOST="cassandra"
CASSANDRA_USER="cassandra"
CASSANDRA_PASSWORD="cassandra"


#Maining log 
#log file name
LOG_FILE_NAME = f"{datetime.now().strftime('%m%d%Y__%H%M%S')}.log"
#log directory
LOG_FILE_DIR = os.path.join(os.getcwd(),"logs")
#create folder if not available
os.makedirs(LOG_FILE_DIR,exist_ok=True)


logging.basicConfig(
    filename=os.path.join(LOG_FILE_DIR,LOG_FILE_NAME),
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


#create spark session with cassandar configuration
sparkSesison = (SparkSession.builder
                 .config("spark.cassandra.connection.host","cassandra")
                 .config("spark.cassandra.auth.username","cassandra")
                 .config("spark.cassandra.auth.password","cassandra")
                 .appName("demo").getOrCreate()
                 )


#Reading table from cassandra db and returning spark dataframe
def dataFrameFromCassandaDbTable(sparkSession:SparkSession,keyspace:str,table:str)->DataFrame:
    df = (sparkSession.read
        .format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=keyspace)
        .load())
    return df

def sendDataToKafkaTopic(kafkaBootstrapServer, topicName, dataFrame:DataFrame):
    logging.info(f"Started writing data to Kafka topic {topicName} and server: {kafkaBootstrapServer}")
    dataFrame = dataFrame.select(col("step"), col("type"), col("amount"), col("name_orig"), col("old_balance_org"),
                                 col("new_balance_orig"), col("name_dest"), col("old_balance_dest"),
                                 col("new_balance_dest"), col("is_fraud"), col("is_flagged_fraud"))
    dataFrame.show(2, truncate=False)
    (dataFrame
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrapServer)
        .option("failOnDataLoss", "false")
        .option("topic", topicName)
        .save())
    logging.info(f"Data has been written to Kafka topic: {topicName}")


if __name__ == "__main__":
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create Spark session
sparkSession = SparkSession.builder.appName("demo").getOrCreate()

# Define the schema for the Cassandra table
cassandraSchema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("name_orig", StringType(), True),
    StructField("old_balance_org", DoubleType(), True),
    StructField("new_balance_org", DoubleType(), True),
    StructField("name_dest", StringType(), True),
    StructField("old_balance_dest", DoubleType(), True),
    StructField("new_balance_dest", DoubleType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("is_flagged_fraud", IntegerType(), True)
])

# Read data from Cassandra database using the specified schema
cassandraDF = sparkSession.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table=TABLE, keyspace=KEYSPACE) \
    .schema(cassandraSchema) \
    .load()

# Show the Cassandra DataFrame schema and data
cassandraDF.printSchema()
cassandraDF.show(truncate=False)

# Select only the required columns
selectedDF = cassandraDF.select(
    col("step"), col("type"), col("amount"), col("name_orig"), col("old_balance_org"),
    col("new_balance_org"), col("name_dest"), col("old_balance_dest"),
    col("new_balance_dest"), col("is_fraud"), col("is_flagged_fraud")
)

# Write data to Kafka topic
sendDataToKafkaTopic(
    kafkaBootstrapServer=KAFKA_BOOTSTRAP_SERVER,
    topicName=KAFKA_TOPIC,
    dataFrame=selectedDF
)

       
