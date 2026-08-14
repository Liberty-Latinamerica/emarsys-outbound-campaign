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
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('2d699190-9da4-46de-bfa1-49ebc19785ce', 'tcl2B-twlMYtTqGD9KAEhnJCkAUa6FLAvfK3BdgiVNc')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = 'bd3f8f3c-1eaa-4416-801e-25a122eff49b'
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
            Message=f'The LCPR export process for WhatsAppID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable-lcpr: Downloading whatsappID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('2d699190-9da4-46de-bfa1-49ebc19785ce', 'tcl2B-twlMYtTqGD9KAEhnJCkAUa6FLAvfK3BdgiVNc')
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
  
    # # Get the download URL
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('2d699190-9da4-46de-bfa1-49ebc19785ce', 'tcl2B-twlMYtTqGD9KAEhnJCkAUa6FLAvfK3BdgiVNc')
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
            Message=f'The LCPR download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable-lcpr: Downloading url for  whatsappID get_download API exception!!!',
        )
    
    # # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final whatsappID details:",content)
      
        # s3://lla-emarsys-mapping/emarsys-lookup/CWP/
        # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/LCPR/whatsappID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("LCPR whatsappID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For LCPR whatsappID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable: LCPR whatsappID file not generated!!!',
        )
        
    # ####################TemplateID#####
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('2d699190-9da4-46de-bfa1-49ebc19785ce', 'tcl2B-twlMYtTqGD9KAEhnJCkAUa6FLAvfK3BdgiVNc')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = '42b506e3-0597-4b8a-9ae9-96454a0f4dbd'
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
            Message=f'The LCPR export process for TemplateID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable: Downloading TemplateID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('2d699190-9da4-46de-bfa1-49ebc19785ce', 'tcl2B-twlMYtTqGD9KAEhnJCkAUa6FLAvfK3BdgiVNc')
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
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('2d699190-9da4-46de-bfa1-49ebc19785ce', 'tcl2B-twlMYtTqGD9KAEhnJCkAUa6FLAvfK3BdgiVNc')
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
            Message=f'The LCPR download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable: Downloading url for  TemplateID get_download API exception!!!',
        )
    
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final TemplateID details:",content)
      
        
    #   # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/LCPR/TemplateID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("TemplateID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For LCPR TemplateID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable: LCPR TemplateID file not generated!!!',
        )

