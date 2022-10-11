import urllib
from spark_conf import sparkconf
from get_insights import get_performance
from get_env import get_env

def lambda_handler(event, context):
    print("Get the bucket and the file name from the event")
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # Load environment variables
    print("Loading the environment variables")
    Env_var = get_env()
    s3_bucket = Env_var.s3_bucket
    s3_filename = Env_var.s3_filename
    # "s3a://get-insights-poc/data/data.tsv"
    s3_loc = "s3a://"+s3_bucket+s3_filename

    print("Loading the spark context")
    sc = sparkconf()
    spark_context = sc.spark
    get_performance(spark_context, s3_loc)

if __name__ == '__main__':
    event = "event"
    context = "context"
    lambda_handler(event, context)