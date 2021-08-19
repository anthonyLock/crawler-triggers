import time

import boto3
from botocore.exceptions import ClientError

class Crawler:
    """
    Glue is the class that will deal with all the glue requests for AWS
    """

    def __init__(self,
                env:str,
                region:str,
                glue_role:str,
                glue_database:str,
                s3_bucket:str,
                new_partition:bool,
                new_bucket:bool):
        self.client = boto3.client("glue", region_name=region)
        self.glue_crawler_name = s3_bucket
        self.s3_bucket = s3_bucket
        self.new_partition = new_partition
        self.new_bucket = new_bucket
        self.glue_role = glue_role
        self.glue_database = glue_database
        self.env = env

    def __wait_for_crawler_to_be_ready(self):
        ready = False
        while not ready: 
            ready = self.__is_crawler_ready()
            time.sleep(10)


    def __is_crawler_ready(self) -> bool: 
        try: 
            response = self.client.get_crawler(
                Name=self.glue_crawler_name
            )
            if response["Crawler"]["State"] == "READY": 
                return True
            else: 
                return False
        except Exception as e:
            raise RuntimeError(f"Failed get state of crawler  {self.glue_crawler_name} - {e}")
            
    def __stop_glue_crawler(self):
        """
        Stop the glue crawler if running, if it is not running then do not give a error.
        """
        try:
            self.client.stop_crawler(
                Name=self.glue_crawler_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'CrawlerNotRunningException': 
                return 
    
            if e.response['Error']['Code'] != 'CrawlerStoppingException':
                raise RuntimeError(f"Failed to stop crawler {self.glue_crawler_name} - {e}")
        
        self.__wait_for_crawler_to_be_ready()
   
    def __create_new_crawler(self):
        try: 
            self.client.create_crawler(
                Name=self.glue_crawler_name,
                Role=self.glue_role,
                DatabaseName=self.glue_database,
                Targets={
                    "S3Targets": [{
                        "Path": "s3://" + self.s3_bucket + "/"
                    }]
                },
                Tags={
                    "Environment": self.env,
                    "ProjectCode": "DEV-WM2DATAPLAT",
                    "ProductCode": "LDP Integration",
                    "BusinessUnit": "woodmac",
                    "Contact": "LensDataTechnologyPlatform@verisk.onmicrosoft.com"
                }
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create crawler  {self.glue_crawler_name} - {e}")
    
    def __start_crawler(self): 
        try: 
            self.client.start_crawler(
                Name= self.glue_crawler_name
            )
        except Exception as e:
            raise RuntimeError(f"Failed to start crawler  {self.glue_crawler_name} - {e}")

    def run(self):
        if (not self.new_partition and not self.new_bucket ): 
            return

        if self.new_bucket:
            self.__create_new_crawler()

        self.__stop_glue_crawler()

        self.__start_crawler()