import time

import boto3
from botocore.exceptions import ClientError

class Crawler:
    """
    Glue is the class that will deal with all the glue requests for AWS
    """

    def __init__(self, region:str, glue_crawler_name:str, s3_bucket:str, new_partition:bool):
        self.client = boto3.client("glue", region_name=region)
        self.glue_crawler_name = glue_crawler_name
        self.s3_bucket = s3_bucket
        self.new_partition = new_partition

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

    def __get_crawlers_s3_buckers(self) -> list:
        try: 
            response = self.client.get_crawler(
                Name=self.glue_crawler_name
            )
            return list(response["Crawler"]["Targets"]["S3Targets"])
        except Exception as e:
            raise RuntimeError(f"Failed get state of crawler  {self.glue_crawler_name} - {e}") 


        
    def __add_s3_to_crawler(self):
        try: 
            s3_buckets = self.__get_crawlers_s3_buckers()
            s3_buckets.append({
                "Path": self.s3_bucket
            })
            self.client.update_crawler(
                Name=self.glue_crawler_name,
                Targets={
                    "S3Targets": s3_buckets
                }
            )
        except Exception as e:
            raise RuntimeError(f"Failed to update crawler  {self.glue_crawler_name} - {e}")
    
    def __start_crawler(self): 
        try: 
            self.client.start_crawler(
                Name= self.glue_crawler_name
            )
        except Exception as e:
            raise RuntimeError(f"Failed to start crawler  {self.glue_crawler_name} - {e}")

    def run(self):
        if (not self.new_partition and self.s3_bucket == "" ): 
            return

        self.__stop_glue_crawler()

        if self.s3_bucket != "":
            self.__add_s3_to_crawler()

        self.__start_crawler()