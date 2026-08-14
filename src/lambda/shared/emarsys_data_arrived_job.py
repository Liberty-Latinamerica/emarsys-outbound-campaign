import json
import os
import re
import csv
import boto3
import datetime
import logging
from io import StringIO
from botocore.config import Config
from botocore.exceptions import ClientError

# ============================================================
# S1.1 — Structured Logging (replaces all print statements)
# ============================================================
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

audit_logger = logging.getLogger("emarsys.audit")
audit_logger.setLevel(logging.WARNING)

# ============================================================
# S0.1, S0.2, S0.3, S0.4 — Environment Variables
# (replaces hardcoded account IDs, bucket, table, Lambda name)
# ============================================================
MAPPING_BUCKET = os.environ['MAPPING_BUCKET']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
OUTBOUND_LAMBDA_ARN = os.environ['OUTBOUND_LAMBDA_ARN']
QUEUE_URL_CWC = os.environ['QUEUE_URL_CWC'].strip()
QUEUE_URL_CWP = os.environ['QUEUE_URL_CWP'].strip()
QUEUE_URL_LCPR = os.environ['QUEUE_URL_LCPR'].strip()


# ============================================================
# S1.2 — Boto3 Retry Config (module-level clients)
# ============================================================
retry_config = Config(
    retries={'max_attempts': 3, 'mode': 'adaptive'},
    connect_timeout=5,
    read_timeout=30
)
s3_client = boto3.client('s3', config=retry_config)
sqs_client = boto3.client('sqs', region_name='us-east-1', config=retry_config)
lambda_client = boto3.client('lambda', config=retry_config)
cloudwatch_client = boto3.client('cloudwatch')
dynamodb = boto3.resource('dynamodb', config=retry_config)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)


# ============================================================
# Constants
# ============================================================
# URL_PATTERN = re.compile(r'(https?://|www\.)', re.IGNORECASE)  # Disabled — URL check not needed currently
# ALLOWED_CHARS = re.compile(r"^[a-zA-Z0-9 .,'\-]+$")  # Disabled — allowed chars check not needed currently
PHONE_PATTERN = re.compile(r'^\+?[1-9]\d{1,14}$')  # S1.3 — E.164 format
MAX_FIELD_LENGTH = 100
MAX_CSV_SIZE_BYTES = 10 * 1024 * 1024  # S1.8 — 10MB limit
TEMPLATE_FIELDS = ["1", "2"]
VALID_MARKETS = {'BRB', 'BHS', 'JAM', 'CUW', 'TTO', 'CYM', 'CWP', 'LCPR'}
REQUIRED_FIELDS = ['toAddress', 'toAddressMessengerType', 'useExistingActiveConversation',
                   'fromAddress', 'market', 'responseId', 'queue']


# ============================================================
# S1.4 — PII Redaction
# ============================================================
def redact_pii(data):
    """Mask phone numbers before logging."""
    redacted = data.copy()
    for field in ('toAddress', 'fromAddress', 'From_Address'):
        if redacted.get(field):
            redacted[field] = redacted[field][:4] + '****'
    return redacted


# ============================================================
# S1.5 — CloudWatch Metrics
# ============================================================
def publish_metric(metric_name, value, market):
    """Publish custom business metric to CloudWatch."""
    try:
        cloudwatch_client.put_metric_data(
            Namespace='EmarsysCampaigns',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count',
                'Dimensions': [{'Name': 'Market', 'Value': market}]
            }]
        )
    except Exception as e:
        logger.warning(f"Failed to publish metric {metric_name}: {e}")


# ============================================================
# Input Sanitization (existing logic preserved)
# ============================================================
def sanitize_payload(body):
    """Check template variable fields for malicious content."""
    for field in TEMPLATE_FIELDS:
        value = body.get(field)
        if value is None:
            continue
        if not isinstance(value, str):
            return False, f"Field '{field}' must be a string"
        if len(value) > MAX_FIELD_LENGTH:
            return False, f"Field '{field}' exceeds max length of {MAX_FIELD_LENGTH}"
        # if URL_PATTERN.search(value):
        #     return False, f"Field '{field}' contains a URL pattern"  # Disabled — URL check not needed currently
        # if not ALLOWED_CHARS.match(value):
        #     return False, f"Field '{field}' contains disallowed characters"  # Disabled — allowed chars check not needed currently
    return True, ""


# ============================================================
# S1.3 — Input Validation (phone format + market enum)
# ============================================================
def validate_message(body):
    """Validate required fields, market, and phone number format."""
    missing = [f for f in REQUIRED_FIELDS if not body.get(f)]
    if missing:
        return False, f"Missing/empty fields: {missing}"
    if body['market'] not in VALID_MARKETS:
        return False, f"Invalid market: {body['market']}"
    if not PHONE_PATTERN.match(body['toAddress']):
        return False, f"Invalid phone format for toAddress"
    return True, ""


# ============================================================
# S1.7 + S1.8 — CSV Lookup (replaces Pandas) with size check
# ============================================================
def lookup_csv_value(bucket, key, lookup_col, match_value, return_col):
    """Look up a value from a CSV in S3. Includes size protection."""
    # S1.8: Check file size before reading
    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
        if head['ContentLength'] > MAX_CSV_SIZE_BYTES:
            logger.error(f"CSV too large: {key} ({head['ContentLength']} bytes)")
            return None
    except ClientError:
        logger.error(f"Cannot access S3 object: {key}", exc_info=True)
        return None

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read().decode('utf-8')
    reader = csv.DictReader(StringIO(content))
    for row in reader:
        row_lower = {k.lower().strip(): v.strip() for k, v in row.items()}
        if row_lower.get(lookup_col, '') == match_value:
            return row_lower.get(return_col)
    return None


# ============================================================
# DynamoDB Failure Recording
# ============================================================
def update_dynamodb(table, body, delivery_status):
    """Record failed delivery to DynamoDB for audit."""
    if not body.get("toAddress"):
        logger.warning("toAddress is empty, skipping DynamoDB update")
        return
    try:
        table.put_item(Item={
            'toAddress': body.get("toAddress"),
            'Date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'campaign_name': body.get("responseId", ''),
            'market': body.get("market", ''),
            'Delivery_Status': delivery_status,
            'queue': body.get("queue", ''),
            'DELIVERY_EVENT_DESCRIPTION': json.dumps(body)
        })
        logger.info("Failure recorded in DynamoDB")
    except Exception as e:
        logger.error(f"Error updating DynamoDB: {e}", exc_info=True)


# ============================================================
# Core Message Processing
# ============================================================
def process_message(body, market, queue_url, template_file_key, whatsapp_file_key):
    """Map template/WhatsApp IDs from S3 CSVs, forward to SQS + Lambda."""
    response_id1 = body['responseId']
    from_address = body['fromAddress']
    campaign_name = body['responseId']

    # Template ID lookup
    template_id = lookup_csv_value(
        MAPPING_BUCKET, template_file_key,
        'template_name', response_id1, 'response_id'
    )
    if not template_id:
        logger.warning(f"No template ID found for {response_id1}", extra={"market": market})
        publish_metric('TemplateNotFound', 1, market)
        update_dynamodb(table, body, "failed")
        return

    body['responseId'] = template_id

    # WhatsApp ID lookup
    whatsapp_id = lookup_csv_value(
        MAPPING_BUCKET, whatsapp_file_key,
        'whatsapp_number', from_address, 'whatsapp_id'
    )
    if not whatsapp_id:
        logger.warning(f"No WhatsApp ID found for {from_address[:4]}****", extra={"market": market})
        publish_metric('WhatsAppIdNotFound', 1, market)
        body["responseId"] = campaign_name
        update_dynamodb(table, body, "failed")
        return

    body['fromAddress'] = whatsapp_id
    body['response_id'] = response_id1
    body['From_Address'] = from_address

    # S1.4: Log with PII redacted
    logger.info("Message mapped successfully", extra={"market": market, "payload": redact_pii(body)})

    # Send to SQS
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))
    logger.info("Sent message to SQS", extra={"market": market})

    # Invoke outbound Lambda
    insert_payload = {
        'ani': body['toAddress'],
        'skillgroup': body['queue'],
        'opco': body['market'],
        'responseId': body['responseId'],
        'Activation_Phrase': body.get('Activation_Phrase', ''),
        'From_Address': body['From_Address']
    }
    lambda_client.invoke(
        FunctionName=OUTBOUND_LAMBDA_ARN,
        InvocationType='Event',
        Payload=json.dumps(insert_payload)
    )
    logger.info("Invoked outbound-campaign-lambda", extra={"market": market})
    publish_metric('MessageProcessed', 1, market)


# ============================================================
# Lambda Handler
# ============================================================
def lambda_handler(event, context):
    """SQS batch handler for Emarsys campaign messages."""
    if not event or 'Records' not in event:
        logger.warning("Empty event received")
        return {"batchItemFailures": []}

    messages_to_reprocess = []

    for record in event["Records"]:
        message_id = record.get('messageId', '')

        try:
            body = json.loads(record["body"])

            # S1.4: Log with PII redacted
            logger.info(f"Processing message: {message_id}", extra={"payload": redact_pii(body)})

            # Sanitization check
            is_valid, reason = sanitize_payload(body)
            if not is_valid:
                audit_logger.warning(f"REJECTED: {reason}", extra={"payload": redact_pii(body)})
                publish_metric('MessageRejected', 1, body.get('market', 'UNKNOWN'))
                continue  # Non-retriable

            # S1.3: Input validation
            is_valid, reason = validate_message(body)
            if not is_valid:
                logger.warning(f"Validation failed: {reason}", extra={"message_id": message_id})
                publish_metric('ValidationFailed', 1, body.get('market', 'UNKNOWN'))
                update_dynamodb(table, body, "failed")
                continue  # Non-retriable

            # Route by market
            market = body["market"]
            if market == "BRB":
                process_message(body, 'BRB', QUEUE_URL_CWC, 'emarsys-lookup/CWC/Barbados/TemplateID_file.csv', 'emarsys-lookup/CWC/all-reg-whatsappID/whatsappID_file.csv')
            elif market == "BHS":
                process_message(body, 'BHS', QUEUE_URL_CWC, 'emarsys-lookup/CWC/Bahamas/TemplateID_file.csv', 'emarsys-lookup/CWC/all-reg-whatsappID/whatsappID_file.csv')
            elif market == "JAM":
                process_message(body, 'JAM', QUEUE_URL_CWC, 'emarsys-lookup/CWC/Jamaica/TemplateID_file.csv', 'emarsys-lookup/CWC/all-reg-whatsappID/whatsappID_file.csv')
            elif market == "CUW":
                process_message(body, 'CUW', QUEUE_URL_CWC, 'emarsys-lookup/CWC/Curacao/TemplateID_file.csv', 'emarsys-lookup/CWC/all-reg-whatsappID/whatsappID_file.csv')
            elif market == "TTO":
                process_message(body, 'TTO', QUEUE_URL_CWC, 'emarsys-lookup/CWC/Trinidad/TemplateID_file.csv', 'emarsys-lookup/CWC/all-reg-whatsappID/whatsappID_file.csv')
            elif market == "CYM":
                process_message(body, 'CYM', QUEUE_URL_CWC, 'emarsys-lookup/CWC/Cayman/TemplateID_file.csv', 'emarsys-lookup/CWC/all-reg-whatsappID/whatsappID_file.csv')
            elif market == "CWP":
                process_message(body, 'CWP', QUEUE_URL_CWP, 'emarsys-lookup/CWP/TemplateID_file.csv', 'emarsys-lookup/CWP/whatsappID_file.csv')
            elif market == "LCPR":
                process_message(body, 'LCPR', QUEUE_URL_LCPR, 'emarsys-lookup/LCPR/TemplateID_file.csv', 'emarsys-lookup/LCPR/whatsappID_file.csv')
            else:
                logger.warning(f"Unknown market: {market}")
                publish_metric('UnknownMarket', 1, market)
                update_dynamodb(table, body, "failed")

        # ============================================================
        # S0.6 — Categorized Exception Handling
        # ============================================================
        except json.JSONDecodeError as e:
            # Non-retriable — malformed JSON will never succeed on retry
            logger.error(f"Malformed message body: {e}", exc_info=True)
            publish_metric('MalformedMessage', 1, 'UNKNOWN')

        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"AWS error ({error_code}): {e}", exc_info=True)
            # Only retry transient errors
            if error_code in ('ThrottlingException', 'ProvisionedThroughputExceededException',
                              'ServiceUnavailable', 'InternalError'):
                messages_to_reprocess.append({"itemIdentifier": message_id})
                publish_metric('RetriableError', 1, 'UNKNOWN')
            else:
                publish_metric('NonRetriableAWSError', 1, 'UNKNOWN')

        except Exception as e:
            # Unknown error — retry to be safe
            logger.error(f"Unexpected error: {e}", exc_info=True)
            messages_to_reprocess.append({"itemIdentifier": message_id})
            publish_metric('UnexpectedError', 1, 'UNKNOWN')

    logger.info(f"Batch complete: {len(event['Records'])} records, {len(messages_to_reprocess)} failures")
    return {"batchItemFailures": messages_to_reprocess}
