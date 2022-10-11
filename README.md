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

## How to run
1) Clone the repository
2) Run the following command to install the libraries/dependencies
    py -m pip install -r requirements.txt
3) Create a .env file to have your local environment variables as below
    access_key_id = "Add your access key id with quotes"
    secret_access_key = "Your secret access key with quotes"
    s3_bucket = "s3 bucket name"
    s3_filename = "path to the input file without the s3 bucket name, including all subdirectories"
3) Run the lambda_handler.py
    py lambda_handler.py


## Questions and Concerns
Please contact me at spudi84@gmail.com for any questions or concerns
