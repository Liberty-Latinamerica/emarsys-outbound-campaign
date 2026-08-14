import boto3
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Genesys_Outbound_Campaign_db')
dynamodb_client = boto3.client('dynamodb')

dynamodb_client = boto3.client('dynamodb', 'us-east-1')
dynamodb = boto3.resource('dynamodb')
customer_db = dynamodb.Table('Genesys_Outbound_Campaign_db')

def lambda_handler(event, context):
    print(event)
    api_response=event['body-json']
    ani = api_response['ANI']

    
    query = 'SELECT * FROM "Genesys_Outbound_Campaign_db" WHERE  ANI = {} AND Record_Status = {}'.format(f"'{ani}'", f"'{'A'}'")
    customer_response = dynamodb_client.execute_statement(Statement=query, ConsistentRead=True)
    print(customer_response)
   
    
    array = []
    token = ""

    array.extend(customer_response['Items'])

    if 'NextToken' in customer_response:

        print('True')
        token = customer_response['NextToken']

        while len(token) > 0:
            
            customer_response = dynamodb_client.execute_statement( Statement=query, ConsistentRead=True, NextToken = token)
            array.append(customer_response['Items'])

            if 'NextToken' in customer_response:
                token = customer_response['NextToken']

            else:
                token = ""

    else:

        print('Full Db is scanned.')


    
    if len(array) > 0:
        for item in array:
            d=item['Record_Status']['S']
            item['Record_Status']['S'] ='D'
         
            
            Record_Status=item['Record_Status']['S']
            partition_key =item['ANI']['S']
            sort_key = item['Campaign_ID']['S']
     
            response = customer_db.update_item(Key={'ANI': partition_key, 'Campaign_ID': sort_key}, 
                                                        UpdateExpression = "SET Record_Status = :val1",
                                                        ExpressionAttributeValues = {':val1': Record_Status})
            print("Record_Status was successfully changed A to D in Genesys_Outbound_Campaign_db DB .")
        return {
            'statusCode': 200,
            'body': json.dumps('Hello record status updated!')
        }
    else:
        print(f"Requested  {ani} is not present in the database.")
        return {
            'statusCode': 200,
            'body': json.dumps('sorry! the ANI is not there')
        }
       

