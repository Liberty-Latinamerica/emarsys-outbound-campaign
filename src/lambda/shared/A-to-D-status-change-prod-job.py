import json
import boto3
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
table_name = 'Genesys_Outbound_Campaign_db'
table = dynamodb.Table(table_name)

# Initialize the SNS client
sns_client = boto3.client('sns')

current_day = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
current_day_year = current_day[0:4]
current_day_month = current_day[4:6]
current_day_day = current_day[6:]
date_string = "{}-{}-{}".format(current_day_year, current_day_month, current_day_day)
yesterday_str = str(date_string)

def lambda_handler(event, context):
    try:
        response = table.scan()
        items = response['Items']
        changed_count = 0

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        filtered_items = [item for item in items if 'Delivery_date' in item and yesterday_str in item['Delivery_date']]
        for item in filtered_items:
            if item.get('Record_Status') == 'A':
                ani = item['ANI']
                camId = item['Campaign_ID']

                table.update_item(
                    Key={'ANI': ani, 'Campaign_ID': camId},
                    UpdateExpression='SET Record_Status = :val',
                    ExpressionAttributeValues={':val': 'D'}
                )
                changed_count += 1
        print("changed_count:", changed_count)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'changed_count': changed_count
            })
        }
    except Exception as e:
        # Handle any exceptions that may occur during DynamoDB operations or other parts of your code.
        print("An error occurred:", str(e))
        
        # Send an SNS notification with the error message
        sns_client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EMARSYS_EXCEPTION',
            Message=f'A to D changing exception. Error: {str(e)}',
            Subject='!!!A-to-D-status-change-prod-job : code got exception!!!',
        )
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error_message': str(e)
            })
        }
