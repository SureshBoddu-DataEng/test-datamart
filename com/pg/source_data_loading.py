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

    src_list = app_conf["source_list"]

    for src in src_list:
        print("src = ", src)
        src_conf = app_conf[src]

        if src == 'SB':
            # Read data from MySQL - TransactionSync table, create dataframe out of it
            # Add a column 'ins_dt' - current_date()
            # Write dataframe in S3 partitioned by 'ins_dt'

            print("\nStart reading data from SB:Mysql and write it S3")

            txn_df = ut.read_from_mysql(spark, src_conf, app_secret) \
                .withColumn("ins_dt", current_date())

            txn_df.show()

            txn_df.write \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"]+"/"+app_conf["s3_conf"]["staging_dir"]+"/"+src)
            print("\nReading data from SB:Mysql and write it S3 is completed")

        elif src == 'OL':
            # Read data from SFTP - receipt_delta, create dataframe out of it
            # Add a column 'ins_dt' - current_date()
            # Write dataframe in S3 partitioned by 'ins_dt'
            print("\nStart reading data from OL:SFTP and write it S3")

            pem_file_path = current_dir + "/../../" + app_secret["sftp_conf"]["pem"]

            ol_txn_df = ut.read_from_sftp(spark, src_conf, app_secret, pem_file_path) \
                .withColumn("ins_dt", current_date())

            ol_txn_df.show(5, False)

            ol_txn_df.write \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"]+"/"+app_conf["s3_conf"]["staging_dir"]+"/"+src)
            print("\nReading data from OL:SFTP and write it S3 is completed")
        elif src == 'CP':
            # Read data from S3 - kc_extract_file, create dataframe out of it
            # Add a column 'ins_dt' - current_date()
            # Write dataframe in S3 partitioned by 'ins_dt'
            print("\nStart reading data from CP:Source S3 and write it destination S3")

            finance_df = ut.read_from_s3(spark, src_conf) \
                .withColumn("ins_dt", current_date())

            finance_df.show(5, False)

            finance_df.write \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"]+"/"+app_conf["s3_conf"]["staging_dir"]+"/"+src)
            print("\nReading data from CP:Source S3 and write it destination S3 is completed")
        elif src == 'ADDR':
            # Read data from MongoDB - , create dataframe out of it
            # Add a column 'ins_dt' - current_date()
            # Write dataframe in S3 partitioned by 'ins_dt'
            print("\nStart reading data from ADDR:MongoDB and write it destination S3")
            addr_df = ut.read_from_mongodb(spark, src_conf, app_secret) \
                .withColumn("ins_dt", current_date())

            addr_df.show(5, False)

            addr_df.write \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"]+"/"+app_conf["s3_conf"]["staging_dir"]+"/"+src)
            print("\nReading data from ADDR:MongoDB and write it destination S3 is completed")
#spark-submit --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.apache.hadoop:hadoop-aws:2.7.4,org.mongodb.spark:mongo-spark-connector_2.11:2.4.2" com/pg/source_data_loading.py
