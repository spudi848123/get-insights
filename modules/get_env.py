import os, sys
from dotenv import load_dotenv

class get_env(object):
    """Load Local Environment variables"""
    def __init__(self) -> None:
        load_dotenv()
        access_key_id = os.getenv('access_key_id')
        secret_access_key = os.getenv('secret_access_key')
        s3_bucket = os.getenv("s3_bucket")
        s3_filename = os.getenv("s3_filename")
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.s3_bucket = s3_bucket
        self.s3_filename = s3_filename