from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, functions
import os, sys
from datetime import date
from urllib.parse import urlparse, parse_qs
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from get_env import get_env

# Set pyspark environment variables 
print("Setting the pyspark environment variables")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class get_performance(object):
    """The method iterates through all the products and their attributes and collects the product_name and 
    its revenue in a products dictionary"""
    def getAllpurchases(self, rawProductList):
        products = {}
        # ip = csvDfRow.ip
        # purchaseDatetime = csvDfRow.date_time
        productsList = rawProductList.split(',')
        for product in productsList:
            productAttr = product.split(';')
            productName = productAttr[1].lower().replace(' ','_')
            numItems = int(productAttr[2])
            # Total Revenue is the number of items times the product price
            revenue = numItems*int(productAttr[3])
            products[productName] = revenue
        return products

    """The below method processes the data and returns the following hits
        1) Search URL Hits
        2) Order purchase hit with the event list = 1"""    
    def getSearchurls(self, csvDfRow):
        ip = csvDfRow.ip
        products = {}
        eventList = csvDfRow.event_list
        rawProductsList = csvDfRow.product_list
        productName = 'None'
        revenue = 0
        searchEng = 'None'
        searchkeyword = 'None'
        date_time = csvDfRow.date_time
        parsed = urlparse(csvDfRow.referrer)
        # Excluding the search URLs with esshopzilla as its the website itself
        if parsed.path == '/search' and parsed.netloc != 'www.esshopzilla.com':
            searchEng = parsed.netloc.split('.')[1]
            if searchEng == 'yahoo':
                searchkeyword = (parse_qs(parsed.query)['p'][0]).lower().replace(' ','_')
            else:
                searchkeyword = (parse_qs(parsed.query)['q'][0]).lower().replace(' ','_')
        # Getting all the purchase hits
        elif eventList == "1":
            print(f"Getting products {rawProductsList}")
            products = self.getAllpurchases(rawProductsList)
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

    def __init__(self, spark_context, s3_loc):
        # Get the object from the event and show its content type
        csvDf = spark_context.read.csv(s3_loc,sep=r'\t', header=True)

        print("Finished reading the data file")

        # Collecting the rows with the search URLs and their purchase hits
        searchEngDF = csvDf.rdd.map(lambda x: self.getSearchurls(x)).toDF(["datetime", "ip","searchEngine","keyWord", "productName", "revenue"])

        # Applying row number over partition by IP address so that the search hits have a row number lower than its purchase hit
        windowSpec  = Window.partitionBy("ip").orderBy("datetime")

        searchPurchaseDF = searchEngDF.filter(~ ( ( searchEngDF.searchEngine == 'None' ) & ( searchEngDF.productName == 'None' ) ) ) \
            .withColumn("row_number",row_number().over(windowSpec))

        # Capturing the current date
        today = date.today().strftime("%Y-%m-%d")
        # target file location on s3
        write_csv = "s3a://get-insights-poc/output/"+today+"_SearchKeywordPerformance"

        # 1) Applying Self join on the dataframe to assign the search hits to its corresponding purchase
        # 2) Grouping by the search Engine and Keyword to calculate the revenue
        # 3) Using the coalesce DataFrame function to consolidate all the data into one file
        (searchPurchaseDF.alias("search").join(searchPurchaseDF.alias("purchase"), \
            col("search.ip") == col("purchase.ip"), "inner") \
                .select(col("search.ip"),col("search.searchEngine"),col("search.keyWord"), \
                    col("purchase.productName"),col("purchase.revenue"), \
                    col("search.datetime").alias("search_datetime"), col("purchase.datetime").alias("purchase_datetime"))\
                .where(col("search.row_number") == col("purchase.row_number")-1)) \
                .groupBy("searchEngine","keyWord").sum("revenue").withColumnRenamed("sum(revenue)","TotalRevenue") \
                .coalesce(1) \
                    .write.options(header='True', delimiter=r'\t').mode("overwrite").format("csv") \
                        .csv(write_csv)