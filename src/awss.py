import boto3

class aws_clients(object):
    def __init__(self) -> None:
        s3_client = boto3.client('s3')
        self.s3_client = s3_client
        