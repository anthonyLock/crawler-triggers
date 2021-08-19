import os
import logging 
import glue

region = os.environ["AWS_REGION"]
glue_crawler_role = os.environ["AWS_GLUE_ROLE"]
glue_database = os.environ["AWS_GLUE_DATABASE"]
environment = os.environ["ENV"]

def handler(event, context = {}):
    try:
        crawler = glue.Crawler(
            environment,
            region,
            glue_crawler_role,
            glue_database,
            event.get("s3_bucket", ""),
            event.get("new_partition",False),
            event.get("new_bucket", False),
            )
        crawler.run()
        return {}
    except Exception as e:
        logging.critical(e)
        raise e

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    handler({})