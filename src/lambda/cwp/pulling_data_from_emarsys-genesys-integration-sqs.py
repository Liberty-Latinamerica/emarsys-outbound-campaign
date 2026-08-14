import boto3
import json
import logging
import time

sqs_client = boto3.client('sqs')
lambda_client = boto3.client('lambda')

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/030615279213/emarsys-genesys-integration-sqs"  
MAX_EXECUTION_TIME = 55 # Stop processing 5 seconds before timeout

def lambda_handler(event, context):
    start_time = time.time()
    messages_to_send = []  
    delete_batch = []      
    received_count = 0     

    try:
        # Step 1: Pull messages from SQS in batches of 10
        while received_count < 900:
            elapsed_time = time.time() - start_time
            if elapsed_time > MAX_EXECUTION_TIME:
                logger.warning("Approaching Lambda timeout. Stopping message processing.")
                break

            response = sqs_client.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5 
            )

            # Check if we received any messages
            if 'Messages' in response:
                for message in response['Messages']:
                    messages_to_send.append(message['Body'])
                    delete_batch.append({
                        'Id': message['MessageId'],
                        'ReceiptHandle': message['ReceiptHandle']
                    })
                    received_count += 1

                    # Delete messages in batches of 10
                    if len(delete_batch) == 10:
                        delete_messages(delete_batch)
                        delete_batch.clear()

                    # Stop receiving more if we have collected 300 messages
                    if received_count >= 900:
                        print("break applied")
                        print("received_count:",received_count)
                        break
            else:
                logger.info("No new messages in SQS. Stopping processing.")
                break

        # Step 2: Delete remaining pulled messages
        if delete_batch:
            delete_messages(delete_batch)
            delete_batch.clear()

        # Step 3: Send pulled messages to Lambda B
        if messages_to_send:
            send_to_outbound_api_call_dev(messages_to_send)
            logger.info(f"Sent {len(messages_to_send)} messages to outbound lambda.")
        else:
            logger.info("No messages to send to outbound lambda.")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")


def delete_messages(delete_batch):
    """
    Deletes messages from SQS in a batch.
    """
    response = sqs_client.delete_message_batch(
        QueueUrl=QUEUE_URL,
        Entries=delete_batch
    )
    # logger.info(f"Deleted {len(delete_batch)} messages from SQS.")
    
    # Log any failures in deletion
    if 'Failed' in response and response['Failed']:
        failed_ids = [item['Id'] for item in response['Failed']]
        logger.error(f"Failed to delete message IDs: {failed_ids}")


def send_to_outbound_api_call_dev(messages_to_send):
    """
    Sends the collected messages to outbound-api-call-dev in batches.
    """
    batch_size = 50  
    for i in range(0, len(messages_to_send), batch_size):
        batch = messages_to_send[i:i + batch_size]
        event_payload = {"outbound_messages": batch}
        
        # Send each batch to outbound-api-call-dev
        response = lambda_client.invoke(
            FunctionName="arn:aws:lambda:us-east-1:030615279213:function:outbound_api_call", 
            InvocationType='Event',  
            Payload=json.dumps(event_payload)
        )
        logger.info(f"Sent batch {i // batch_size + 1} to outbound lambda.")
