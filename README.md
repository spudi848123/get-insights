# get-insights
## Description
Given a data file with the website hit data, the pyspark job returns a tab delimited file with following columns
    1) Search Engine Domain - The search engine used to search an item
    2) Search Keyword - Keyword used to search in the search engine
    3) Revenue - Total Revenue of the search engine and the keyword

## Assumptions made: 
1) Users are not directly searching on eshopzilla
2) This is daily data

## AWS Stack for the problem
The problem can be best solved by using the following AWS Services
1) S3 - To contain the input and output files
2) Lambda - To run the pyspark job
3) ECR - To run the pyspark job on docker

## Logic Explanation
1) Read the csv file into a RDD DataFrame
2) Iterate through the file to 
    1. Get all the records of the search urls (Anything that contains the string "search?" but not eshopzilla)
    2. Parse the query string to get the Search Engine, Key Word
    2. Get all records for the purchases made (event List = 1) 
3) Write to a DataFrame. The headers in the DataFrame look as below

        > date_time          | ip         | searchEngine | searchkeyword | productName | revenue
        > 2009-09-27 6:37:58 | 23.8.61.21 | bing         | ipod          | None        | 0
        > 2009-09-27 6:42:55 | 23.8.61.21 | None         | None          | ipod_touch  | 100
4) Revenue is calculated by (num items*list price)
5) Apply row number over partition by IP address, order by date_time so that the search hits have a row number lower than its purchase hit

        > date_time          | ip         | searchEngine | searchkeyword | productName | revenue | row_number
        > 2009-09-27 6:37:58 | 23.8.61.21 | bing         | ipod          | None        | 0       | 1
        > 2009-09-27 6:42:55 | 23.8.61.21 | None         | None          | ipod_touch  | 100     | 2

6) Apply Self join on the dataframe to assign the search hits to its corresponding purchase

        > date_time          | ip         | searchEngine | searchkeyword | productName | revenue 
        > 2009-09-27 6:37:58 | 23.8.61.21 | bing         | ipod          | ipod_touch  | 100
        
7) Group by the search Engine and Keyword to calculate the sum of revenue
8) Apply **coalesce** on DataFrame function to consolidate all the data into one file
9) Write the csv to the destination s3 bucket. 
    *I wasn't able to figure out a way to write the csv file directly with the naming convention. An alternative way is to access the output file again and rename it.*


## How to run
1) Clone the repository
2) Run the following command to install the libraries/dependencies
    > py -m pip install -r requirements.txt
3) Create a .env file to have your local environment variables as below
    access_key_id = "Add your access key id with quotes"
    secret_access_key = "Your secret access key with quotes"
    s3_bucket = "s3 bucket name"
    s3_filename = "path to the input file without the s3 bucket name, including all subdirectories"
3) Run the lambda_handler.py
    > py lambda_handler.py


## Questions and Concerns
Please contact me at spudi84@gmail.com for any questions or concerns
