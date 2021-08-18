# Crawler triggers 

Crawler triggers is a lambda that is gets invoked from a kafka topic. 
If a new bucket is added then the glue crawler gets updated and then set to run 
If a new partition in a bucket is added then the glue crawler runs 
If no new partition and no new bucket then the glue crawler doesn't run. 

## Requirements 
Must have boto3,coverage and pytest package in the python path 

## Testing 
We are using pytest to run the tests. 
```
make test
```
if you want a visual representation of the coverage run 
```
make coverage
```
