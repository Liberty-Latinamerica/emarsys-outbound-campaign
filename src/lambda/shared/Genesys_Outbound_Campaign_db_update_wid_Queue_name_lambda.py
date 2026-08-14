import boto3
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Genesys_Outbound_Campaign_db')
dynamodb_client = boto3.client('dynamodb')

dynamodb_client = boto3.client('dynamodb', 'us-east-1')
dynamodb = boto3.resource('dynamodb')
customer_db = dynamodb.Table('Genesys_Outbound_Campaign_db')

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
    
def lambda_handler(event, context):
    print(event)
    api_response=event['body-json']
    ani = api_response['ANI']
     
    response_db=[]
    if 'Campaign_ID' in api_response and 'Activation_Phrase' in api_response:
        campaign_id=api_response['Campaign_ID']
        activation_phrase=api_response['Activation_Phrase']
        query = 'SELECT * FROM "Genesys_Outbound_Campaign_db" WHERE  ANI = {} AND Record_Status = {} AND Campaign_ID= {} AND Activation_Phrase= {}'.format(f"'{ani}'", f"'{'A'}'", f"'{campaign_id}'",f"'{activation_phrase}'")
        response_db=dynamodb_query(query)
        print(response_db)
   
    if len(response_db) > 0:
        for item in response_db:
            item['Record_Status']['S'] ='D'
            
            Record_Status=item['Record_Status']['S']
            partition_key =item['ANI']['S']
            sort_key = item['Campaign_ID']['S']
            print(partition_key)
            
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
            
   
