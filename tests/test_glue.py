import unittest
from unittest import mock

from glue import Crawler
from botocore.stub import Stubber

class TestCrawler(unittest.TestCase):
    def test_constructor(self):
        """
        Test that the constructor saves the correct outcomes. 
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new-bucket", False)

        assert crawler.new_partition == False
        assert crawler.glue_crawler_name == "test-crawler"
        assert crawler.s3_bucket == "new-bucket"

class TestCrawlerRun(unittest.TestCase):    
    def test_run_do_not_start(self):
        """
        No New s3 bucket and no new partitions so no crawler activity needed 
        """
        crawler = Crawler("eu-west-1", "test-crawler", "", False)
        with mock.patch.object( crawler, "_Crawler__stop_glue_crawler") as stop_glue_crawler_func, \
            mock.patch.object( crawler, "_Crawler__start_crawler") as start_crawler_func, \
            mock.patch.object( crawler, "_Crawler__add_s3_to_crawler") as add_s3_to_crawler_func :                
            
            crawler.run()
            stop_glue_crawler_func.assert_not_called()
            add_s3_to_crawler_func.assert_not_called()
            start_crawler_func.assert_not_called()

    def test_run_do_not_add_s3_bucket(self):
        """
        No New s3 bucket and But new partitions so crawler needs to be restarted 
        """
        crawler = Crawler("eu-west-1", "test-crawler", "", True)
        with mock.patch.object( crawler, "_Crawler__stop_glue_crawler") as stop_glue_crawler_func, \
            mock.patch.object( crawler, "_Crawler__start_crawler") as start_crawler_func, \
            mock.patch.object( crawler, "_Crawler__add_s3_to_crawler") as add_s3_to_crawler_func :                
            
            crawler.run()
            add_s3_to_crawler_func.assert_not_called()
            stop_glue_crawler_func.assert_called_once()
            start_crawler_func.assert_called_once()

    def test_run_add_s3_bucket(self):
        """
        New s3 bucket and But new partitions so crawler needs to be restarted 
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        with mock.patch.object( crawler, "_Crawler__stop_glue_crawler") as stop_glue_crawler_func, \
            mock.patch.object( crawler, "_Crawler__start_crawler") as start_crawler_func, \
            mock.patch.object( crawler, "_Crawler__add_s3_to_crawler") as add_s3_to_crawler_func :                
            
            crawler.run()
            add_s3_to_crawler_func.assert_called_once()
            stop_glue_crawler_func.assert_called_once()
            start_crawler_func.assert_called_once()            
    
class TestCrawlerAddS3Buckets(unittest.TestCase):    
    def test_add_s3_to_crawler(self):
        """
        Test adding the s3 bucket from the event to the crawler 
        """
        s3_bucket_retrun = [
            {
                "Path": "s3_bucket1",
            },
            {
                "Path": "s3_bucket2",
            }
        ]
        s3_bucket_expected = {"S3Targets":[
            {
                "Path": "s3_bucket1",
            },
            {
                "Path": "s3_bucket2",
            },
            {
                "Path": "new_s3_bucket",
            },
        ]}
        
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        with mock.patch.object( crawler, "_Crawler__get_crawlers_s3_buckers", return_value=s3_bucket_retrun) as get_crawlers_s3_buckers, \
            mock.patch.object( crawler.client, "update_crawler") as update_crawler:
            crawler._Crawler__add_s3_to_crawler()
            get_crawlers_s3_buckers.assert_called_once()
            update_crawler.assert_called_once_with(Name="test-crawler", Targets=s3_bucket_expected)

    def test_add_s3_to_crawler_update_fail(self):
        """
        Test adding the s3 bucket from the event to the crawler 
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("update_crawler", service_error_code ="UpdateCrawlerFailed", service_message="aws-fail")
        with stubber,\
        mock.patch.object( crawler, "_Crawler__get_crawlers_s3_buckers", return_value=[]) as get_crawlers_s3_buckers,\
        self.assertRaises(RuntimeError) as cm:
            crawler._Crawler__add_s3_to_crawler()
        self.assertEqual(cm.exception.__str__(), 'Failed to update crawler  test-crawler - An error occurred (UpdateCrawlerFailed) when calling the UpdateCrawler operation: aws-fail')         
        

class TestCrawlerStartCrawler(unittest.TestCase):    
    def test_start_crawler_pass(self):
        """
        Test to check the happy path of start crawler  
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        with mock.patch.object( crawler.client, "start_crawler") as start_crawler_func:
            crawler._Crawler__start_crawler()
            start_crawler_func.assert_called_once_with(Name="test-crawler")    
         

    def test_start_crawler_fail(self):
        """
        Test to check the error message from the start crawler  
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("start_crawler", service_error_code ="StartCrawlerFailed", service_message="aws-fail")
        with stubber,\
        self.assertRaises(RuntimeError) as cm:
            crawler._Crawler__start_crawler()
        self.assertEqual(cm.exception.__str__(), 'Failed to start crawler  test-crawler - An error occurred (StartCrawlerFailed) when calling the StartCrawler operation: aws-fail')         


class TestCrawlerGetS3Buckets(unittest.TestCase):    
    def test_get_s3_buckets_happy_path(self):
        """
        Test to check the happy path of get s3 buckets  
        """
        s3 =  [
                {
                    "Path": "s3_bucket1",
                },
                {
                    "Path": "s3_bucket2",
                }
            ]
        s3_bucket_retrun = {
            'Crawler': {
                'Targets': {
                    "S3Targets": s3
                }
            }
        }
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        with mock.patch.object( crawler.client, "get_crawler", return_value = s3_bucket_retrun) as get_crawler_func:
            res = crawler._Crawler__get_crawlers_s3_buckers()
            get_crawler_func.assert_called_once_with(Name="test-crawler")
            self.assertEqual(res, s3)    
         

    def test_get_s3_buckets_fail(self):
        """
        Test to check the error message from get s3 Buckets  
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("get_crawler", service_error_code ="GetCrawlerFailed", service_message="aws-fail")
        with stubber,\
        self.assertRaises(RuntimeError) as cm:
            crawler._Crawler__get_crawlers_s3_buckers()
        self.assertEqual(cm.exception.__str__(), 'Failed get state of crawler  test-crawler - An error occurred (GetCrawlerFailed) when calling the GetCrawler operation: aws-fail')         
     

class TestCrawlerStopCrawler(unittest.TestCase):    
    def test_stop_crawler_happy_path(self):
        """
        Test calling stop crawler happy path.   
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        stubber = Stubber(crawler.client)
        stubber.add_response("stop_crawler", {}, {"Name":"test-crawler"})
        with stubber,\
        mock.patch.object( crawler, "_Crawler__wait_for_crawler_to_be_ready") as wait_for_crawler_func:
            crawler._Crawler__stop_glue_crawler()
            wait_for_crawler_func.assert_called_once()

    def test_stop_crawler_not_running(self):
        """
        Test calling stop crawler when then crawler is not running.   
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("stop_crawler", service_error_code ="CrawlerNotRunningException")
        with stubber,\
        mock.patch.object( crawler, "_Crawler__wait_for_crawler_to_be_ready") as wait_for_crawler_func:
            crawler._Crawler__stop_glue_crawler()
            wait_for_crawler_func.assert_not_called()

    def test_stop_crawler_stopping(self):
        """
        Test calling stop crawler when then crawler is already stopping.   
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("stop_crawler", service_error_code ="CrawlerStoppingException")
        with stubber,\
        mock.patch.object( crawler, "_Crawler__wait_for_crawler_to_be_ready") as wait_for_crawler_func:
            crawler._Crawler__stop_glue_crawler()
            wait_for_crawler_func.assert_called_once()
            
    def test_stop_crawler_failing(self):
        """
        Test calling stop crawler fail.   
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("stop_crawler", service_error_code ="CrawlerStoppingFailed", service_message="aws-fail")
        with stubber,\
        mock.patch.object( crawler, "_Crawler__wait_for_crawler_to_be_ready") as wait_for_crawler_func, \
        self.assertRaises(RuntimeError) as cm:
            crawler._Crawler__stop_glue_crawler()
            wait_for_crawler_func.assert_called_once()
        
        self.assertEqual(cm.exception.__str__(), 'Failed to stop crawler test-crawler - An error occurred (CrawlerStoppingFailed) when calling the StopCrawler operation: aws-fail')         

class TestCrawlerIsCrawlerReady(unittest.TestCase):    
    def test_is_crawler_ready_happy_path(self):
        """
        Test calling __is_crawler_ready happy path
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        stubber = Stubber(crawler.client)
        stubber.add_response("get_crawler", {
            "Crawler": {
                "State": "READY"
            }
        }, {"Name":"test-crawler"})
        with stubber:
            res  = crawler._Crawler__is_crawler_ready()
            self.assertEqual(res, True)
        stubber.add_response("get_crawler", {
            "Crawler": {
                "State": "RUNNING"
            }
        }, {"Name":"test-crawler"})
        with stubber:
            res  = crawler._Crawler__is_crawler_ready()
            self.assertEqual(res, False)            
    def test_is_crawler_ready_fail(self):
        """
        Test calling __is_crawler_ready fail.   
        """
        crawler = Crawler("eu-west-1", "test-crawler", "new_s3_bucket", True)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("get_crawler", service_error_code ="GetCrawlerFailed", service_message="aws-fail")
        with stubber,\
        self.assertRaises(RuntimeError) as cm:
            crawler._Crawler__is_crawler_ready()
        self.assertEqual(cm.exception.__str__(), 'Failed get state of crawler  test-crawler - An error occurred (GetCrawlerFailed) when calling the GetCrawler operation: aws-fail')         
