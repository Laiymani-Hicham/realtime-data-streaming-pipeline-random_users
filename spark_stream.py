from cassandra.io.asyncioreactor import AsyncioConnection
import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

''' 
    create_spark_connection()     → start Spark
    connect_to_kafka()            → connect Spark to Kafka
    create_selection_df_from_kafka() → structure the raw Kafka data
    create_cassandra_connection() → connect to Cassandra
    create_keyspace()             → create Cassandra database
    create_table()                → create Cassandra table
'''
def create_spark_connection():
    s_conn = None #we strat with failure, if everything works it changes, if not we print out the error
#SparkSession.builder is just like KafkaProducer, we use it to start spark.
#appName is only the name of spark we are going to see in the UI.
#each spark needs packages, 
# here we need packages for cassandra and kafka sql since we are going to read from kafka and then send data to cassandra.
#to tell spark where cassandra lives, why not kafka? because we are going to do it in an independent function later, 
# and spark does need to connect to kafka from the start up it, it will once it wants to read from it unlike cassandra.
#getorCreate to Ignore: only exectues the connection
    try:

        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming')  \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                               "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")  \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate() 
    except Exception as error:
        logging.error(f"Couldn't create the spark session due to exception {error}")
    return s_conn


def connect_spark_to_kafka(spark_conn): #connect to kafka needs a spark session to start with, thats why we pass spark_conn
    spark_df = None
    try:
        # tells spark to read continuously without stopping
        # format = source (kafka)
        # bootstrap servers = where kafka lives
        # subscribe = which topic to listen to
        # startingOffsets = tells spark: once connected to kafa read from the very first message
        #.load() To ignore(just executes the connection)
        spark_df = spark_conn.readStream  \
            .format('kafka')  \
            .option('kafka.bootstrap.servers', 'localhost:9092')  \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load() 
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
    #we create the schema for the data cus spark doesnt understand the byte values in kafka.
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    #Cast here is a SQL expression thats why we use it inside selectExpr and not select which is used for python,
    #the data we are going to find in kafka contains lot of infos, the values them selves, and other metadata like dates of reception topic ..., we only need values so thats why we select it 
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*") 
    # from_json parses the JSON string into a structured object using the schema                                                    
    # (each field gets its name and type from the schema we defined)
    # .alias('data') gives that structured object a temporary name 'data'
    # .select("data.*") explodes 'data' into individual separate columns
    print(sel)

    return sel


def create_cassandra_connection():
    try:
        from cassandra.io.asyncioreactor import AsyncioConnection
        cluster = Cluster(['localhost'], connection_class=AsyncioConnection)
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
    return None


def create_keyspace(session): #keyspace in cassandra is the equivalent of a database in Postgres or Mysql
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    #Cassandra doesnt understand python, thats why we use session.execute => its the way to communicate with cassandra
    #it tells it : Hey Cassandra, take this CQL and execute it
    # ALWAYS THIS SYNTAXE: keyspace_name.table_name
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")



if __name__ == "__main__": #as we said before, we wrap the executing function inside __name__ = __main__ 
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_spark_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")
    #this streming_query bloc is to write data from kafka to cassandra
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()