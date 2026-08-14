import json
import boto3
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
table_name = 'Genesys_Outbound_Campaign_db'
table = dynamodb.Table(table_name)

# Initialize the SNS client
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    try:
        response = table.scan()
        items = response['Items']
        changed_count = 0

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        filtered_response = [item for item in items if item.get('Delivery_date') == '']
        for item in filtered_response:
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
        
        print("An error occurred:", str(e))
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
