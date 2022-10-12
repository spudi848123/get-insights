from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from get_env import get_env

class sparkconf:
    def __init__(self) -> None:
        # Get environment variables
        Env_var = get_env()
        # Create the Spark Context
        spark = SparkSession.builder.appName("adobe_get_insights").getOrCreate()
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", Env_var.access_key_id)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", Env_var.secret_access_key)
        # spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4')
        spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "false") 
        spark._jsc.hadoopConfiguration().set("fs.s3a.multiobjectdelete.enable", "false")
        self.spark = spark