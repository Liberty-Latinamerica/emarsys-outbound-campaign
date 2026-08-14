
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
    #########dowloading WhatsappId for cwc all region###############
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = 'c9436bb7-b59f-4199-bb22-748ebf12483f'
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
            Message=f'The CWC export process for WhatsAppID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading whatsappID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
            Message=f'The CWC download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading url for  whatsappID get_download API exception!!!',
        )
        
    
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final whatsappID details:",content)
      
  
    # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/CWC/all-reg-whatsappID/whatsappID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("CWC whatsappID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For CWC whatsappID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: CWC whatsappID file not generated!!!',
        )
        
    #########dowloading TemplateID_file for cwc["Bahamas"]###############
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = '749f65cc-e5ff-4b0e-83a6-cb34e1ff7da8'
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
            Message=f'The BHS export process for TemplateID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading TemplateID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
            Message=f'The BHS download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading url for  TemplateID get_download API exception!!!',
        )
    
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final TemplateID details:",content)
      
        
    #   # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/CWC/Bahamas/TemplateID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("Bahamas TemplateID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For BHS TemplateID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Bahamas TemplateID file not generated!!!',
        )
    #########dowloading TemplateID_file for cwc["Barbados"]###############
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = '430a594d-7293-4b06-93aa-1245051eb1e2'
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
            Message=f'The BRB export process for TemplateID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable: Downloading TemplateID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
            Message=f'The BRB download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading url for  TemplateID get_download API exception!!!',
        )
    
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final TemplateID details:",content)
      
        
    #   # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/CWC/Barbados/TemplateID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("Barbados TemplateID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For BRB TemplateID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Barbados TemplateID file not generated!!!',
        )

    #########dowloading TemplateID_file for cwc["Curacao"]###############
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = 'f6c7999f-0014-4613-a390-55ae01d67b98'
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
            Message=f'The CUW export process for TemplateID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading TemplateID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
            Message=f'The CUW download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading url for  TemplateID get_download API exception!!!',
        )
    
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final TemplateID details:",content)
      
        
    #   # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/CWC/Curacao/TemplateID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("Curacao TemplateID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For CUW TemplateID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Curacao TemplateID file not generated!!!',
        )
    #########dowloading TemplateID_file for cwc["Jamaica"]###############
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = '3fe5992e-5d60-4a19-a2e2-57a7eb3627d3'
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
            Message=f'The JAM export process for TemplateID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading TemplateID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
            Message=f'The JAM download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading url for  TemplateID get_download API exception!!!',
        )
    
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final TemplateID details:",content)
      
        
    #   # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/CWC/Jamaica/TemplateID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("Jamaica TemplateID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For JAM TemplateID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Jamaica TemplateID file not generated!!!',
        )
    #########dowloading TemplateID_file for cwc["Trinidad"]###############
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = '035ee048-3b0e-4831-a83f-32ca97b58e63'
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
            Message=f'The TTO export process for TemplateID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable: Downloading TemplateID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
            Message=f'The TTO download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading url for  TemplateID get_download API exception!!!',
        )
    
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final TemplateID details:",content)
      
        
    #   # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/CWC/Trinidad/TemplateID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("Trinidad TemplateID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For TTO TemplateID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Trinidad TemplateID file not generated!!!',
        )
    #########dowloading TemplateID_file for cwc["cayman"]###############
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    
    # Create an instance of the API class
    api_instance = PureCloudPlatformClientV2.ArchitectApi()
    
    # Post - Begin an export process for exporting all rows from a datatable
    datatable_id = '94a7599e-d3a6-403e-971c-5ff709db15f4'
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
            Message=f'The cym export process for TemplateID API calls is getting an exception.Input parameters is:{str(datatable_id)}  ,error : ' + str(e),
            Subject='!!!genesys-dataTable: Downloading TemplateID post_flows_datatable_export_jobs API exception!!!',
        )
    
    # Get - Returns the state information about an export job
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token('0a5866b1-1a48-4143-a84b-7e3faaffa75a','jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
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
            Message=f'The cym download URL API calls are getting an exception.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: Downloading url for  TemplateID get_download API exception!!!',
        )
    
    # Download the CSV file
    try:
        response = urllib.request.urlopen(download_url)
        content=response.read()
        print("final TemplateID details:",content)
      
        
    #   # Upload the file to S3
        s3_bucket = 'lla-emarsys-mapping' 
        s3_key = 'emarsys-lookup/CWC/Cayman/TemplateID_file.csv' 
        # Save the CSV file to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        print("cayman TemplateID CSV file downloaded successfully.")
    except Exception as e:
        print("Exception when downloading the CSV file: %s\n" % e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'For TTO TemplateID CSV file not downloaded successfully.,error : ' + str(e),
            Subject='!!!genesys-dataTable-cwc: cayman TemplateID file not generated!!!',
        )
    
