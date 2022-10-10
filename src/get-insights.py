# from math import prod
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, functions
import os, csv, codecs, sys
from awss import aws_clients
from datetime import date
from urllib.parse import urlparse, parse_qs
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DecimalType
# from pyspark.sql.types import ArrayType, DoubleType, BooleanType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

access_key_id = "AKIA3EW3WXFASEQZGJGT"
secret_access_key = "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"

# spark = SparkSession.builder.appName("adobe_get_insights").config("spark.jars", "x.jar,y.jar").getOrCreate()
spark = SparkSession.builder.appName("adobe_get_insights").getOrCreate()
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_access_key)
# spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
# spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
# spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "false") 
spark._jsc.hadoopConfiguration().set("fs.s3a.multiobjectdelete.enable", "false")

csvDf = spark.read.csv("s3a://get-insights-poc/data/data.tsv",sep=r'\t', header=True)

# csvDf = spark.read.csv("data.tsv",sep=r'\t', header=True)
print("got csvDF")

def getAllpurchases(rawProductList):
    products = {}
    # ip = csvDfRow.ip
    # purchaseDatetime = csvDfRow.date_time
    productsList = rawProductList.split(',')
    for product in productsList:
        productAttr = product.split(';')
        productName = productAttr[1].lower().replace(' ','_')
        numItems = int(productAttr[2])
        revenue = numItems*int(productAttr[3])
        products[productName] = revenue
    return products
    # for k, v in products.items():
    #     return (purchaseDatetime,ip,k,v)

def getSearchurls(csvDfRow):
    ip = csvDfRow.ip
    products = {}
    eventList = csvDfRow.event_list
    rawProductsList = csvDfRow.product_list
    productName = 'None'
    revenue = 0
    searchEng = 'None'
    searchkeyword = 'None'
    date_time = csvDfRow.date_time
    print(f"Processign row with IP {ip} and datetime {date_time}")
    parsed = urlparse(csvDfRow.referrer)
    if parsed.path == '/search' and parsed.netloc != 'www.esshopzilla.com':
        searchEng = parsed.netloc.split('.')[1]
        if searchEng == 'yahoo':
            searchkeyword = (parse_qs(parsed.query)['p'][0]).lower().replace(' ','_')
        else:
            searchkeyword = (parse_qs(parsed.query)['q'][0]).lower().replace(' ','_')
    elif eventList == "1":
        print(f"Getting products {rawProductsList}")
        products = getAllpurchases(rawProductsList)
        print(f"Products : {products}")
    
    if products:
        for k, v in products.items():
            print(f"Returning products list {k},{v}")
            print(f"datetime {date_time}, IP {ip}, searchEng {searchEng}, searchkeyword {searchkeyword}, productName {k}, revenue {v}")
            return(date_time, ip, searchEng, searchkeyword, k, v)
    elif searchEng:
        print(f"Returning searchEng {searchEng}")
        return(date_time, ip, searchEng, searchkeyword, productName, revenue)
    else:
        pass
    print(f"Exiting out of getSearchurls")

def write_csv_with_specific_file_name(sc, df, path, filename):
    df.repartition(1).write.options(header='True', delimiter=r'\t').mode("overwrite").format("csv").save(path)
    try:
        sc_uri = sc._gateway.jvm.java.net.URI
        sc_path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        file_system = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
        fs = file_system.get(sc_uri("hdfs://localhost:8020"), configuration())
        src_path = None
        status = fs.listStatus(sc_path(path))
        for fileStatus in status:
            temp = fileStatus.getPath().toString()
            if "part" in temp:
                src_path = sc_path(temp)
        dest_path = sc_path(path + filename)
        if fs.exists(src_path) and fs.isFile(src_path):
            fs.rename(src_path, dest_path)
            fs.delete(src_path, True)
    except Exception as e:
        raise Exception("Error renaming the part file to {}:".format(filename, e))

# filteredDf = csvDf.filter("event_list = 1" | "urlparse(referrer).path = '/search'")
searchEngDF = csvDf.rdd.map(lambda x: getSearchurls(x)).toDF(["datetime", "ip","searchEngine","keyWord", "productName", "revenue"])

windowSpec  = Window.partitionBy("ip").orderBy("datetime")

searchPurchaseDF = searchEngDF.filter(~ ( ( searchEngDF.searchEngine == 'None' ) & ( searchEngDF.productName == 'None' ) ) ) \
    .withColumn("row_number",row_number().over(windowSpec))

today = date.today().strftime("%Y-%m-%d")
# write_csv = "s3a://get-insights-poc/output/"+today
write_csv = "output"
output_file_name = "/"+today+"_SearchKeywordPerformance.tab"
# write_csv = today+"_SearchKeywordPerformance"

# (searchPurchaseDF.alias("search").join(searchPurchaseDF.alias("purchase"), \
#     col("search.ip") == col("purchase.ip"), "inner") \
#         .select(col("search.ip"),col("search.searchEngine"),col("search.keyWord"), \
#             col("purchase.productName"),col("purchase.revenue"), \
#             col("search.datetime").alias("search_datetime"), col("purchase.datetime").alias("purchase_datetime"))\
#         .where(col("search.row_number") == col("purchase.row_number")-1)) \
#         .groupBy("searchEngine","keyWord").sum("revenue").withColumnRenamed("sum(revenue)","TotalRevenue") \
#         .coalesce(1) \
#             .write.options(header='True', delimiter=r'\t').mode("overwrite").format("csv") \
#                 .csv(write_csv)
searchKWPerfDF = (searchPurchaseDF.alias("search").join(searchPurchaseDF.alias("purchase"), \
                col("search.ip") == col("purchase.ip"), "inner") \
                    .select(col("search.ip"),col("search.searchEngine"),col("search.keyWord"), \
                        col("purchase.productName"),col("purchase.revenue"), \
                        col("search.datetime").alias("search_datetime"), col("purchase.datetime").alias("purchase_datetime"))\
                    .where(col("search.row_number") == col("purchase.row_number")-1)) \
                    .groupBy("searchEngine","keyWord").sum("revenue").withColumnRenamed("sum(revenue)","TotalRevenue") \
                    .coalesce(1)

write_csv_with_specific_file_name(spark.sparkContext, searchKWPerfDF, write_csv, output_file_name)
# schema = StructType() \
#       .add("SearchEngine",StringType(),False) \
#       .add("Keyword",StringType(),False) \
#       .add("Revenue",DecimalType(),False)

#  hdfs://localhost:8020/

# clients = aws_clients()

# s3_client = clients.s3_client

# response = s3_client.get_object(Bucket='get-insights-poc', Key='data/data.tsv')

# body = response.get('Body')
# # print(response.get('Body'))

# for ln in codecs.getreader('utf-8')(body):
#     print(ln)

