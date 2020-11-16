from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import yaml
import os.path
import com.pg.utils.utility as ut
import uuid

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config("spark.mongodb.input.uri", 'mongodb://ec2-52-17-243-155.eu-west-1.compute.amazonaws.com:27017') \
        .config("spark.mongodb.output.uri", 'mongodb://ec2-52-17-243-155.eu-west-1.compute.amazonaws.com:27017') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    def fn_uuid():
        uid = uuid.uuid1()
        return uid

    FN_UUID = spark.udf\
                   .register("FN_UUID", fn_uuid, StringType())

    target_list = app_conf["target_list"]

    for tgt in target_list:
        tgt_conf = app_conf[tgt]
        if tgt == 'REGIS_DIM':
            src_list = tgt_conf["sourceData"]

            for src in src_list:
                print("src = "+src)
                print("Redading from S3   >>>>>>>")
                df = spark.read \
                    .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src) \
                    .repartition(5)

                df.show(5, False)
                df.createOrReplaceTempView(src)

            print("REGIS_DIM")
            spark.sql(tgt_conf["loadingQuery"]).show(5, False)

            regDimDf = spark.sql(tgt_conf["loadingQuery"])

            ut.write_data_to_redshift(regDimDf.coalesce(1),
                                      app_secret,
                                      "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                      tgt_conf["tableName"])

        elif tgt == 'CHILD_DIM':
            src_list = tgt_conf["sourceData"]

            for src in src_list:
                print("src = " + src)
                print("Redading from S3   >>>>>>>")
                df = spark.read \
                    .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src) \
                    .repartition(5)

                df.show(5, False)
                df.createOrReplaceTempView(src)

            print("CHILD_DIM")
            spark.sql(tgt_conf["loadingQuery"]).show(5, False)

            childDimDf = spark.sql(tgt_conf["loadingQuery"])

            ut.write_data_to_redshift(childDimDf.coalesce(1),
                                      app_secret,
                                      "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                      tgt_conf["tableName"])

        elif tgt == 'RTL_TXN_FCT':
            src_list = tgt_conf["sourceData"]

            for src in src_list:
                print("src = " + src)
                print("Redading from S3   >>>>>>>")
                dimDf = spark.read \
                    .parquet(
                    "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src) \
                    .repartition(5)

                dimDf.show(5, False)
                dimDf.createOrReplaceTempView(src)

            src_tbl = tgt_conf["sourceTable"]
            for src in src_tbl:
                print("src = " + src)
                print("Redading from S3   >>>>>>>")
                regDimDf = ut.read_data_from_redshift(spark,
                                                      app_secret,
                                                      "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                                      app_conf)\
                             .repartition(5)

                regDimDf.show(5, False)
                regDimDf.createOrReplaceTempView(src)

            print("RTL_TXN_FCT")
            spark.sql(tgt_conf["loadingQuery"]).show(5, False)

            ut.write_data_to_redshift(childDimDf.coalesce(1),
                                      "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                      tgt_conf["tableName"])

#spark-submit --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.apache.hadoop:hadoop-aws:2.7.4,org.mongodb.spark:mongo-spark-connector_2.11:2.4.2" --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py
