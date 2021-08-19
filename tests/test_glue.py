import unittest
from unittest import mock

from glue import Crawler
from botocore.stub import Stubber

class TestCrawler(unittest.TestCase):
    def test_constructor(self):
        """
        Test that the constructor saves the correct outcomes. 
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, True)

        self.assertEqual(crawler.glue_crawler_name, "glue_s3_bucket")
        self.assertEqual(crawler.s3_bucket, "glue_s3_bucket")
        self.assertEqual(crawler.new_partition, True)
        self.assertEqual(crawler.new_bucket, True)
        self.assertEqual(crawler.glue_role, "glue_role")
        self.assertEqual(crawler.glue_database, "glue_db")
        self.assertEqual(crawler.env, "dev")

class TestCrawlerRun(unittest.TestCase):    
    def test_run_do_not_start(self):
        """
        No New s3 bucket and no new partitions so no crawler activity needed 
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", False, False)
        with mock.patch.object( crawler, "_Crawler__stop_glue_crawler") as stop_glue_crawler_func, \
            mock.patch.object( crawler, "_Crawler__start_crawler") as start_crawler_func, \
            mock.patch.object( crawler, "_Crawler__create_new_crawler") as create_new_crawler_func :                
            
            crawler.run()
            stop_glue_crawler_func.assert_not_called()
            create_new_crawler_func.assert_not_called()
            start_crawler_func.assert_not_called()

    def test_run_do_not_add_s3_bucket(self):
        """
        No New s3 bucket and But new partitions so crawler needs to be restarted 
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
        with mock.patch.object( crawler, "_Crawler__stop_glue_crawler") as stop_glue_crawler_func, \
            mock.patch.object( crawler, "_Crawler__start_crawler") as start_crawler_func, \
            mock.patch.object( crawler, "_Crawler__create_new_crawler") as create_new_crawler_func :                
            
            crawler.run()
            create_new_crawler_func.assert_not_called()
            stop_glue_crawler_func.assert_called_once()
            start_crawler_func.assert_called_once()

    def test_run_add_s3_bucket(self):
        """
        New s3 bucket and new partitions so crawler needs to be restarted 
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, True)
        with mock.patch.object( crawler, "_Crawler__stop_glue_crawler") as stop_glue_crawler_func, \
            mock.patch.object( crawler, "_Crawler__start_crawler") as start_crawler_func, \
            mock.patch.object( crawler, "_Crawler__create_new_crawler") as create_new_crawler_func :                
            
            crawler.run()
            create_new_crawler_func.assert_called_once()
            stop_glue_crawler_func.assert_called_once()
            start_crawler_func.assert_called_once()            
    
class TestCrawlerCreateCrawler(unittest.TestCase):    
    def test_create_crawler_happy_path(self):
        """
        Test calling __create_new_crawler happy path
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
        stubber = Stubber(crawler.client)
        stubber.add_response("create_crawler", {}, 
        {
            "Name":"glue_s3_bucket",
            "Role": "glue_role",
            "DatabaseName": "glue_db",
            "Targets": {
                "S3Targets": [{
                    "Path": "s3://glue_s3_bucket/"
                }]
            },
            "Tags": {
                "Environment": "dev",
                "ProjectCode": "DEV-WM2DATAPLAT",
                "ProductCode": "LDP Integration",
                "BusinessUnit": "woodmac",
                "Contact": "LensDataTechnologyPlatform@verisk.onmicrosoft.com"
            }
        })
        with stubber:
            crawler._Crawler__create_new_crawler()    

    def test_create_crawler_fail(self):
        """
        Test calling __create_new_crawler fail.   
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("create_crawler", service_error_code ="CreateCrawlerFailed", service_message="aws-fail")
        with stubber,\
        self.assertRaises(RuntimeError) as cm:
            crawler._Crawler__create_new_crawler()
        self.assertEqual(cm.exception.__str__(), 'Failed to create crawler  glue_s3_bucket - An error occurred (CreateCrawlerFailed) when calling the CreateCrawler operation: aws-fail')         


class TestCrawlerStartCrawler(unittest.TestCase):    
    def test_start_crawler_pass(self):
        """
        Test to check the happy path of start crawler  
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
        with mock.patch.object( crawler.client, "start_crawler") as start_crawler_func:
            crawler._Crawler__start_crawler()
            start_crawler_func.assert_called_once_with(Name="glue_s3_bucket")    
         

    def test_start_crawler_fail(self):
        """
        Test to check the error message from the start crawler  
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("start_crawler", service_error_code ="StartCrawlerFailed", service_message="aws-fail")
        with stubber,\
        self.assertRaises(RuntimeError) as cm:
            crawler._Crawler__start_crawler()
        self.assertEqual(cm.exception.__str__(), 'Failed to start crawler  glue_s3_bucket - An error occurred (StartCrawlerFailed) when calling the StartCrawler operation: aws-fail')         

class TestCrawlerStopCrawler(unittest.TestCase):    
    def test_stop_crawler_happy_path(self):
        """
        Test calling stop crawler happy path.   
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
        stubber = Stubber(crawler.client)
        stubber.add_response("stop_crawler", {}, {"Name":"glue_s3_bucket"})
        with stubber,\
        mock.patch.object( crawler, "_Crawler__wait_for_crawler_to_be_ready") as wait_for_crawler_func:
            crawler._Crawler__stop_glue_crawler()
            wait_for_crawler_func.assert_called_once()

    def test_stop_crawler_not_running(self):
        """
        Test calling stop crawler when then crawler is not running.   
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
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
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
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
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("stop_crawler", service_error_code ="CrawlerStoppingFailed", service_message="aws-fail")
        with stubber,\
        mock.patch.object( crawler, "_Crawler__wait_for_crawler_to_be_ready") as wait_for_crawler_func, \
        self.assertRaises(RuntimeError) as cm:
            crawler._Crawler__stop_glue_crawler()
            wait_for_crawler_func.assert_called_once()
        
        self.assertEqual(cm.exception.__str__(), 'Failed to stop crawler glue_s3_bucket - An error occurred (CrawlerStoppingFailed) when calling the StopCrawler operation: aws-fail')         

class TestCrawlerIsCrawlerReady(unittest.TestCase):    
    def test_is_crawler_ready_happy_path(self):
        """
        Test calling __is_crawler_ready happy path
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
        stubber = Stubber(crawler.client)
        stubber.add_response("get_crawler", {
            "Crawler": {
                "State": "READY"
            }
        }, {"Name":"glue_s3_bucket"})
        with stubber:
            res  = crawler._Crawler__is_crawler_ready()
            self.assertEqual(res, True)
        stubber.add_response("get_crawler", {
            "Crawler": {
                "State": "RUNNING"
            }
        }, {"Name":"glue_s3_bucket"})
        with stubber:
            res  = crawler._Crawler__is_crawler_ready()
            self.assertEqual(res, False)            
    def test_is_crawler_ready_fail(self):
        """
        Test calling __is_crawler_ready fail.   
        """
        crawler = Crawler( "dev", "eu-west-1", "glue_role","glue_db", "glue_s3_bucket", True, False)
        stubber = Stubber(crawler.client)
        stubber.add_client_error("get_crawler", service_error_code ="GetCrawlerFailed", service_message="aws-fail")
        with stubber,\
        self.assertRaises(RuntimeError) as cm:
            crawler._Crawler__is_crawler_ready()
        self.assertEqual(cm.exception.__str__(), 'Failed get state of crawler  glue_s3_bucket - An error occurred (GetCrawlerFailed) when calling the GetCrawler operation: aws-fail')         
