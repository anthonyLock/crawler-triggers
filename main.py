import os
import logging 
import glue

glue_crawler_name = os.environ["GLUE_CRAWLER_NAME"]
region = os.environ["AWS_REGION"]


def handler(event, context = {}):
    try:
        crawler = glue.Crawler(
            region,
            glue_crawler_name, 
            event.get("s3_bucket", ""),
            event.get("new_partition",False),
            )
        crawler.run()
        return {}
    except Exception as e:
        logging.critical(e)
        raise e

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    handler({})