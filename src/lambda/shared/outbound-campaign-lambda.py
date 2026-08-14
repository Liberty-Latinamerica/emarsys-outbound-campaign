import boto3

def lambda_handler(event, context):
    print(event)

    ani = event['ani']
    skill_group = event['skillgroup']
    response_id = event['responseId']
    opco = event['opco']
    Activation_Phrase = event['Activation_Phrase']
    From_Address = event['From_Address']
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Genesys_Outbound_Campaign_db') 
    
    
    if skill_group.strip() == '' or skill_group == '""':
        record_status = 'D' 
    else :
        record_status='A'
    
    item = {
    'ANI': ani,
    'Queue_name': skill_group,
    'Campaign_ID': response_id,
    'Opco': opco,
    'Activation_Phrase': Activation_Phrase,
    'Campaign_duration_time_hours': '',
    'Delivery_date': '',
    'Record_Status': record_status,
    'From_Address': From_Address }

    
    table.put_item(Item=item)
    print("Data stored successfully")
    
    return {
        'statusCode': 200,
        'body': 'Data stored in DynamoDB successfully!'
    }
