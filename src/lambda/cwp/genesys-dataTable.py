
import json
import boto3
import urllib.request
from pprint import pprint
from datetime import datetime, timedelta
from dateutil import tz

# Genesys Cloud SDK imports
import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException


def lambda_handler(event, context):
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('7f9b8168-1c55-4d8d-9007-b4faa3bd220e', 'WKmIXVIIHG4L01AuZtb_oW1xdQt9AdPtNiFj3r3hR4w')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = '45143930-31c4-4d01-a729-dc9e940a82a6'
    print("whatsappID_datatable_id:",datatable_id)
    try:
        api_response = api_instance.post_flows_datatable_export_jobs(datatable_id)
        api_response = api_response.to_dict()
        print("export process for whatsappID",api_response)
    except ApiException as e:
        print("Exception when calling ArchitectApi->post_flows_datatable_export_jobs: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'The CWP export process for WhatsAppID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable: Downloading whatsappID post_flows_datatable_export_jobs API exception!!!',
        )
    
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('7f9b8168-1c55-4d8d-9007-b4faa3bd220e', 'WKmIXVIIHG4L01AuZtb_oW1xdQt9AdPtNiFj3r3hR4w')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    export_job_id = api_response['id']
    print("export_job_id",export_job_id)
    status = "Processing"
    while status == "Processing":
        try:
            api_response = api_instance.get_flows_datatable_export_job(datatable_id,export_job_id)
            api_response = api_response.to_dict()
            print("Returns the state information for whatsappID:",api_response)
            status = api_response['status']
        except ApiException as e:
            print("Exception when calling ArchitectApi->get_flows_datatable_export_job: %s\n" % e)
            
    dwnl_id = api_response['download_uri'].split("/")[-1]
    # Get the download URL
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('7f9b8168-1c55-4d8d-9007-b4faa3bd220e', 'WKmIXVIIHG4L01AuZtb_oW1xdQt9AdPtNiFj3r3hR4w')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    download_id=dwnl_id
    try:
        api_instance_downloads = PureCloudPlatformClientV2.DownloadsApi()
        api_response = api_instance_downloads.get_download(download_id, issue_redirect=False, redirect_to_auth=True)
        api_response = api_response.to_dict()
        download_url = api_response['url']
    except ApiException as e:
        print("Exception when calling DownloadsApi->get_download: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'The CWP download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable: Downloading url for  whatsappID get_download API exception!!!',
        )
        
        
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final whatsappID details:",content)
      
        # s3://lla-emarsys-mapping/emarsys-lookup/CWP/
        # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/CWP/whatsappID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("whatsappID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For CWP whatsappID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable: CWP whatsappID file not generated!!!',
        )
        
        
    ####################TemplateID for CWP###########################
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('7f9b8168-1c55-4d8d-9007-b4faa3bd220e', 'WKmIXVIIHG4L01AuZtb_oW1xdQt9AdPtNiFj3r3hR4w')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = '896675cf-bb54-4cef-9528-ab14dd5ca3ad'
    print("TemplateID_datatable_id:",datatable_id)
    try:
        api_response = api_instance.post_flows_datatable_export_jobs(datatable_id)
        api_response = api_response.to_dict()
        print("export process for TemplateID:",api_response)
    except ApiException as e:
        print("Exception when calling ArchitectApi->post_flows_datatable_export_jobs: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'The CWP export process for TemplateID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable: Downloading TemplateID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('7f9b8168-1c55-4d8d-9007-b4faa3bd220e', 'WKmIXVIIHG4L01AuZtb_oW1xdQt9AdPtNiFj3r3hR4w')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    export_job_id = api_response['id']
    print("export_job_id",export_job_id)
    status = "Processing"
    while status == "Processing":
        try:
            api_response = api_instance.get_flows_datatable_export_job(datatable_id,export_job_id)
            api_response = api_response.to_dict()
            print("Returns the state information for TemplateID:",api_response)
            status = api_response['status']
        except ApiException as e:
            print("Exception when calling ArchitectApi->get_flows_datatable_export_job: %s\n" % e)
            
    dwnl_id = api_response['download_uri'].split("/")[-1]
    # Get the download URL
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('7f9b8168-1c55-4d8d-9007-b4faa3bd220e', 'WKmIXVIIHG4L01AuZtb_oW1xdQt9AdPtNiFj3r3hR4w')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    download_id=dwnl_id
    try:
        api_instance_downloads = PureCloudPlatformClientV2.DownloadsApi()
        api_response = api_instance_downloads.get_download(download_id, issue_redirect=False, redirect_to_auth=True)
        api_response = api_response.to_dict()
        download_url = api_response['url']
    except ApiException as e:
        print("Exception when calling DownloadsApi->get_download: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'The CWP download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable: Downloading url for  TemplateID get_download API exception!!!',
        )
    
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final TemplateID details:",content)
        
       # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/CWP/TemplateID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("TemplateID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For CWP TemplateID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable: CWP TemplateID file not generated!!!',
        )

