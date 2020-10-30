from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col
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
    addr_df.show(5, False)

    flattened_df = addr_df.select(col("consumer_id"), col("mobile-id"), col("address"))
    flattened_df.show()

    spark.stop()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/pg/utils/WriteMongoDBData.py
