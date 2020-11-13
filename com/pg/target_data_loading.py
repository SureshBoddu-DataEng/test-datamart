from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
import yaml
import os.path
import com.pg.utils.utility as ut

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("Process Started.................................")
    src_list = app_conf["REGIS_DIM"]["sourceData"]
    print(src_list)
    for src in src_list:
        print("src = "+src)
        if src == 'CP':
            print("Redading from S3   >>>>>>>")
            txnDf = spark.read \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + app_conf["s3_conf"]["staging_dir"] + "/" + src) \
                .repartition(5)

            txnDf.show(5, False)



    #
    # print("Writing txn_fact dataframe to AWS Redshift Table   >>>>>>>")
    # jdbcUrl = ut.get_redshift_jdbc_url(app_secret)
    # print(jdbcUrl)
    #
    # txnDf.coalesce(1).write\
    #     .format("io.github.spark_redshift_community.spark.redshift") \
    #     .option("url", jdbcUrl) \
    #     .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
    #     .option("forward_spark_s3_credentials", "true") \
    #     .option("dbtable", "PUBLIC.TXN_FCT") \
    #     .mode("overwrite")\
    #     .save()
    #
    # print("Completed   <<<<<<<<<")

#spark-submit --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.apache.hadoop:hadoop-aws:2.7.4,org.mongodb.spark:mongo-spark-connector_2.11:2.4.2" com/pg/target_data_loading.py
