import json
import uuid
import time
from datetime import datetime
import boto3
import PureCloudPlatformClientV2

# Initialize AWS resources
lambda_client = boto3.client('lambda')
dynamodb = boto3.resource('dynamodb')
historical_table = dynamodb.Table('emarsys_outbound_history_table')
cam_db = dynamodb.Table('Genesys_Outbound_Campaign_db')
repo_db = dynamodb.Table('emarsys-genesys-join-db')


def insert_historical_info(table, body, status, info):
    try:
        date_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table.put_item(Item={
            'toAddress': body.get("to_address"),
            'Date': date_string,
            'campaign_name': body.get("response_id"),
            'market': body.get("market",''),
            'delivery_status': status,
            'DELIVERY_EVENT_DESCRIPTION': info,
            'conversationId': body.get("conversation_id", ''),
        })
        print("Historical info inserted in DB")
    except Exception as update_exception:
        print("Error updating DynamoDB item:", update_exception)
        
def update_dynamodb(table, body, status, info):
    try:
        date_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table.put_item(Item={
            'toAddress': body.get("to_address"),
            'Date': date_string,
            'campaign_name': body.get("response_id"),
            'fromAddress': body.get("from_address"),
            'market': body.get("market",''),
            'Delivery_Status': status,
            'queue': body.get("queue", ''),
            'templateId': body.get("responseId"),
            'DELIVERY_EVENT_DESCRIPTION': info,
            'conversationId': body.get("conversation_id", ''),
        })

        print("Report info inserted in DB")
    except Exception as update_exception:
        print("erro info updating DynamoDB item:", update_exception)

def failures_update_dynamodb(table, body, status, info):
    try:
        date_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table.put_item(Item={
            'toAddress': body.get("toAddress"),
            'Date': date_string,
            'campaign_name': body.get("responseId"),
            'market': body.get("market",''),
            'Delivery_Status': status,
            'queue': body.get("queue", ''),
            'DELIVERY_EVENT_DESCRIPTION': info,
        })

        print("Outbound failure report info inserted in DB")
    except Exception as update_exception:
        print("Outbound failure info updating DynamoDB item:", update_exception)

def get_msg_status(data, api_instance):
    """Fetch message status and update DynamoDB."""
    api_response = api_instance.get_conversations_message(data["conversation_id"])
    msg_status = api_response.participants[0].messages[0].message_status
    error_info = api_response.participants[0].messages[0].error_info
    msg_time = api_response.participants[0].messages[0].message_time
    Delivery_date = msg_time.isoformat()

    print(f"Message Status: {msg_status}")

    # Check if the message status is one of the success statuses
    if msg_status in ["delivery-success", "sent", "published", "read"]:
        # Insert success info into DynamoDB

        update_dynamodb(repo_db, data, msg_status, error_info)
        insert_historical_info(historical_table, data, msg_status, error_info)
        print("Success info updated in emarsys-genesys-join-db and emarsys_outbound_history_table.")
    else:
        error_message = error_info.message if error_info else "Unknown error"
        print(f"Delivery failed: {error_message}")
        
        # Insert erro info into DynamoDB
        update_dynamodb(repo_db, data, msg_status, error_message)
        insert_historical_info(historical_table, data, msg_status, error_message)
        print("Failed info updated in emarsys-genesys-join-db and emarsys_outbound_history_table.")

    return Delivery_date


def lambda_handler(event, context):
    event_uuid = str(uuid.uuid4())
    print(f"Received Event (UUID: {event_uuid}): {event}")
    
    apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(
      '0a5866b1-1a48-4143-a84b-7e3faaffa75a', 'jXxQQGuMbBVf8cDgLemi2qwxbyiqqv9S3ywLd8f-pg0')
    PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token

    # Configure OAuth2 access token for authorization: PureCloud OAuth
    api_instance = PureCloudPlatformClientV2.ConversationsApi()
    event_data = event

    for data in event_data["success"]:
        try:
            # Simulate delay for processing
            time.sleep(5)

            # Fetch message status and update DynamoDB
            msg_date = get_msg_status(data, api_instance)

            # Update Text body into Genesys_Outbound_Campaign_db
            cam_db.update_item(
                Key={'ANI': data['to_address'], 'Campaign_ID': data["responseId"]},
                UpdateExpression='SET textbody = :val1, Delivery_date = :val2',
                ExpressionAttributeValues={':val1': data["text_body"], ':val2': msg_date}
            )
            print("Text body and delivery date updated in Genesys_Outbound_Campaign_db.")
            
            ####Invoke the contact list lambda####
            triggering_json = {'responseId':data["responseId"],'opco':data["market"]}
            response = lambda_client.invoke(
                FunctionName='arn:aws:lambda:us-east-1:030615279213:function:create_update_contactlist_to_emarsys_cwc',
                InvocationType='Event',
                Payload=json.dumps(triggering_json),
                )
            print("lambda sccuessfull invoked create_update_contactlist_to_emarsys")

        except Exception as e:
            print(f"Exception occurred: {e}")

   
    for data in event_data["failures"]:
        data["fromAddress"] = data.pop("From_Address")
        data["responseId"] = data.pop("response_id")
        
        error_info = json.dumps(data)
        msg_status = "failed"
        failures_update_dynamodb(repo_db, data, msg_status, error_info)
        
        print("Outbound failures info inserted in emarsys-genesys-join-db")
