from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col
from pyspark.sql.types import StringType
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config("spark.mongodb.input.uri", 'mongodb://ec2-3-249-19-15.eu-west-1.compute.amazonaws.com:27017') \
        .config("spark.mongodb.output.uri", 'mongodb://ec2-3-249-19-15.eu-west-1.compute.amazonaws.com:27017') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    addr_df = spark.read \
        .json("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/KC_Extract_2_20171009.json")

    addr_df.printSchema()
    addr_df = addr_df.select(col("consumer_id"), explode(col("address")).alias("address"), col("mobile-no"))
    addr_df.show(5, False)

    addr_df\
        .write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .mode("append")\
        .option("database", app_conf["ADDR"]["mongodb_config"]["database"])\
        .option("collection", app_conf["ADDR"]["mongodb_config"]["collection"])\
        .save()

    spark.stop()

# spark-submit --packages "rg.mongodb.spark:mongo-spark-connector_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/utils/WriteMongoDBData.py
