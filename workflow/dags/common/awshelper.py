try:
    import boto3
    import os
    import sys
    import datetime
    import concurrent.futures
    import requests
    import json
except Exception as e:
    print("Modules are Missing : {} ".format(e))


AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "")
BUCKET = os.getenv("BUCKET", "")


class AWSS3(object):

    __slots__ = ["BucketName", "client"]

    def __init__(self, BucketName = BUCKET):
        self.BucketName = BucketName
        self.client = boto3.client("s3",
                                   aws_access_key_id=AWS_ACCESS_KEY,
                                   aws_secret_access_key=AWS_SECRET_KEY,
                                   region_name=AWS_REGION_NAME)

    def put_files(self, Key=None, Body=None):
        try:

            response = self.client.put_object(
                ACL='private',
                Body=bytes(Body),
                Bucket=self.BucketName,
                Key=Key)


            print("Happy")



        except Exception as e:
            print("Error : {} ".format(e))

    def putFiles(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:

            Response = json.dumps(Response)
            response = self.client.put_object(
                ACL='private',
                Body=bytes(json.dumps(Response).encode("utf-8")),
                Bucket=self.BucketName,
                Key=Key)
            return response
        except Exception as e:
            print("Error : {} ".format(e))
            return {"Error":str(e)}

    def ItemExists(self, Key):
        try:
            # get the Response for teh Current File
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def getItem(self, Key):
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()
        except Exception as e:
            return False

    def operation(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.ItemExists(Key=key)
        if flag:
            data = self.getItem(Key=key)
            return data
        else:
            self.putFiles(Key=key, Response=data)
            return data




