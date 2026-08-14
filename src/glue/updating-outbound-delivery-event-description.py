import pandas as pd
import json
import time
from datetime import datetime, date, timedelta
import boto3
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize the GlueContext and SparkContext
glueContext = GlueContext(SparkContext.getOrCreate())
logger = glueContext.get_logger()


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('emarsys-genesys-join-db')

######emarsys-genesys-join-db-client
dynamodb_client = boto3.client('dynamodb', 'us-east-1')
s3_client = boto3.client('s3')

def dynamodb_query(query):
    
    array = []  

    response = dynamodb_client.execute_statement(Statement=query, ConsistentRead=True)

    array.append(response['Items'])

    if 'NextToken' in response:
        token = response['NextToken']

        while len(token) > 0:
            response = dynamodb_client.execute_statement(Statement=query, ConsistentRead=True, NextToken=token)
            array.append(response['Items'])

            if 'NextToken' in response:
                token = response['NextToken']
            else:
                token = ""

    else:
        print('Full Db is scanned.')

    data = []

    for list_item in array:
        for record in list_item:
            data.append(record)

    return data
    
def transform_response(response):
    transformed_data = []
    for item in response:
        msg_id = item.get('msg_id', {}).get('S', '')
        conversationId = item.get('conversationId', {}).get('S', '')
        toAddress = item.get('toAddress', {}).get('S', '')
        Date = item.get('Date', {}).get('S', '')
        market = item.get('market', {}).get('S', '')
        Delivery_Status=item.get('Delivery_Status',{}).get('S','')
        transformed_item = {
            'msg_id': msg_id,
            'conversationId': conversationId,
            'toAddress': toAddress,
            'Date': Date,
            'market': market,
            'Delivery_Status':Delivery_Status
        }
        transformed_data.append(transformed_item)
    return transformed_data

def update_delivery_status(api_instance, group):
    for item in group:
        conversationId = item['conversationId']
        toAddress = item['toAddress']
        Date = item['Date']
        try:
            api_response = api_instance.get_conversations_message(conversationId)
            participants = api_response.participants
            if participants and participants[0].messages:
                msg_status = participants[0].messages[0].message_status
                error_info = participants[0].messages[0].error_info
                error_message = error_info.message if error_info else ''
                
                table.update_item(
                    Key={'toAddress': toAddress, 'Date': Date},
                    UpdateExpression='SET DELIVERY_EVENT_DESCRIPTION = :val1, Delivery_Status = :val2',
                    ExpressionAttributeValues={
                        ':val1': error_message,
                        ':val2': msg_status
                    }
                )
        except ApiException as e:
            print(f"Exception when calling ConversationsApi->get_conversations_message: {e}\n")

try:
    date_strings = []
    for i in range(1, 5):
        day_minus_i = (datetime.today() - timedelta(days=i)).strftime("%Y%m%d")
        current_day_year = day_minus_i[0:4]
        current_day_month = day_minus_i[4:6]
        current_day_day = day_minus_i[6:]
        date_string = "{}-{}-{}".format(current_day_year, current_day_month, current_day_day)
        date_strings.append(date_string)
    
    query = 'SELECT "msg_id","conversationId","toAddress","Date","market","Delivery_Status" FROM "emarsys-genesys-join-db" WHERE ' + ' OR '.join([f'contains("Date", \'{date}\')' for date in date_strings])
    print("Query:", query)
    response = dynamodb_query(query)
    
    transformed_response = transform_response(response)
    filtered_response = [item for item in transformed_response if item['conversationId'] and item['Delivery_Status'] not in ('replied', 'read', 'delivery-failed')]
    print("Count of 4 days data:",len(filtered_response)) 
    
    
    market_groups = {}
    for item in filtered_response:
        market_groups.setdefault(item['market'], []).append(item)

    credentials = {
        "CWP": ('92edaaa5-a1b2-481b-bf84-61431422cdbe', 'll4884fL5N8_WoLXYjhOAUQdX4Gu6-PuasIMVUUMM2w'),
        "LCPR": ('2d699190-9da4-46de-bfa1-49ebc19785ce', 'tcl2B-twlMYtTqGD9KAEhnJCkAUa6FLAvfK3BdgiVNc'),
        "BHS": ('0a5866b1-1a48-4143-a84b-7e3faaffa75a', 'jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0'),
        "BRB": ('0a5866b1-1a48-4143-a84b-7e3faaffa75a', 'jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0'),
        "CUW": ('0a5866b1-1a48-4143-a84b-7e3faaffa75a', 'jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0'),
        "JAM": ('0a5866b1-1a48-4143-a84b-7e3faaffa75a', 'jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0'),
        "TTO": ('0a5866b1-1a48-4143-a84b-7e3faaffa75a', 'jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0'),
        "CYM": ('0a5866b1-1a48-4143-a84b-7e3faaffa75a', 'jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
    }


    for market, group in market_groups.items():
        if market in credentials:
            secret_key, secret_token = credentials[market]
            apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(secret_key, secret_token)
            PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
            api_instance = PureCloudPlatformClientV2.ConversationsApi()
            update_delivery_status(api_instance, group)
        else:
            print("Invalid Market")

except Exception as e:
    print("exception", e)
