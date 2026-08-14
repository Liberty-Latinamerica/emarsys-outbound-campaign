import json
import uuid
import boto3
import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException

# Initialize AWS resources
lambda_client = boto3.client('lambda')
sqs_client = boto3.client('sqs')


def send_to_sqs(record):
    response = sqs_client.send_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/030615279213/Emarsys_genesys_integration_sqs_lcpr",
        MessageBody=json.dumps(record)
    )
    print(f"Sent rate-limited event to SQS")


def send_sns_notification(message, subject):
    client = boto3.client('sns')
    response = client.publish(
        TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
        Message=message,
        Subject=subject,
    )
    print("SNS notification sent.")


def lambda_handler(event, context):
    event_uuid = str(uuid.uuid4())
    print(f"Received Event (UUID: {event_uuid}): {event}")

    total_records = len(event["outbound_messages"])
    print(f"{event_uuid} total_records:", total_records)

    api_instance_exception = False
    try:
        apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(
            '2d699190-9da4-46de-bfa1-49ebc19785ce', 'tcl2B-twlMYtTqGD9KAEhnJCkAUa6FLAvfK3BdgiVNc')
        PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
    except ApiException as e:
        if "rate limit exceeded" in str(e):
            api_instance_exception = True
        print(f"{event_uuid} - Rate limit exceeded, sending SNS notification.")
        send_sns_notification(f'outbound_api_call_lcpr an exception: {str(e)}',
                              '!!! outbound_api_call_lcpr : Rate limit exceeded exception!!!')

    dynamodb_update = []
    dynamodb_exception_update = []
    send_to_sqs_list = []
    for record in event["outbound_messages"]:
        if api_instance_exception:
            send_to_sqs_list.append(record)
            data = json.loads(record)
            send_to_sqs(data)
            continue
        try:
            data = json.loads(record)
            excluded_keys = {"INTENT_NAME", "log_uuid", "toAddress", "toAddressMessengerType", "fromAddress", "responseId",
                             "useExistingActiveConversation", "market", "queue", "response_id","From_Address"}
            parameters = [{"id": k, "value": v} for k, v in data.items() if k not in excluded_keys]
            messagingTemplate = {"responseId": data["responseId"], "parameters": parameters}

            body = PureCloudPlatformClientV2.SendAgentlessOutboundMessageRequest()
            body.from_address = data["fromAddress"]
            body.to_address = data["toAddress"]
            body.to_address_messenger_type = data["toAddressMessengerType"]
            body.messaging_template = messagingTemplate
            body.use_existing_active_conversation = data["useExistingActiveConversation"]

            api_instance = PureCloudPlatformClientV2.ConversationsApi()
            api_response = api_instance.post_conversations_messages_agentless(body)
            json_data = api_response.to_dict()
            fields = ["id", "conversation_id", "from_address", "to_address", "messenger_type", "text_body",
                      "use_existing_active_conversation"]
            filtered_data = {key: json_data.get(key) for key in fields}
            # FIX: Handle None messaging_template in Genesys API response
            messaging_template = json_data.get("messaging_template") or {}
            filtered_data["responseId"] = messaging_template.get("response_id")
            filtered_data["response_id"] = data["response_id"]
            filtered_data["market"] = data["market"]
            filtered_data["queue"] = data["queue"]
            filtered_data["From_Address"] = data["From_Address"]
            dynamodb_update.append(filtered_data)
            print(f"Genesys API Response: {filtered_data}")

        except Exception as e:
            print(f"{event_uuid} Exception occurred: {e}")
            try:
                error_body = getattr(e, 'body', None)
                if error_body:
                    response_body = json.loads(error_body)
                    error_message = response_body.get('error_description', response_body.get('message', None))
                    if "rate limit exceeded" in str(error_message).lower():
                        send_sns_notification('outbound_api_call_lcpr an exception', '!!! Rate limit exceeded exception!!!')
                        send_to_sqs_list.append(record)
                        send_to_sqs(data)
                    else:
                        data["error"] = error_message
                        dynamodb_exception_update.append(data)
                else:
                    # No body attribute on exception - log the error and add to failures
                    data["error"] = str(e)
                    dynamodb_exception_update.append(data)
                    print(f"{event_uuid} Exception without body: {str(e)}")
            except Exception as inner_e:
                # JSON parsing failed - still capture the failure
                data["error"] = str(e)
                dynamodb_exception_update.append(data)
                print(f"{event_uuid} Failed to parse exception body: {str(inner_e)}")

    payload = {"success": dynamodb_update, "failures": dynamodb_exception_update}
    if payload["success"] or payload["failures"]:
        try:
            response = lambda_client.invoke(
                FunctionName='arn:aws:lambda:us-east-1:030615279213:function:outbound_data_updates_lcpr',
                InvocationType='Event',
                Payload=json.dumps(payload),
            )
        except Exception as e:
            print(e)

    print(f"Processing Summary (UUID: {event_uuid}): Total: {total_records}, Success: {len(dynamodb_update)}, Failures: {len(dynamodb_exception_update)}, SQS: {len(send_to_sqs_list)}")
