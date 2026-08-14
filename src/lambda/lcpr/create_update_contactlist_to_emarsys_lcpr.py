import json
import boto3
import base64, requests, hashlib, uuid, json
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
# Query the DynamoDB table for all items
table = dynamodb.Table('emarsys_outbound_history_table.')


# LCPR credentials
username = "lilac009"
secret = "X0VgSy0jaiGzf2GJGhsF"

# Funtion to create the X-WSSE header, which is generated from your username and secret
def get_auth(username, secret):
    # Nonce: A random value ensuring that the request is unique, so it cannot be replicated by any other unknown party.
    # This string is always 16 bytes long and must be represented as a 32-character hexadecimal value.
    nonce = uuid.uuid4().hex

    # The current timestamp in ISO8601 format.
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    raw_password_digest = nonce + timestamp + secret

    encrypted_password_digest = hashlib.sha1()
    encrypted_password_digest.update(raw_password_digest.encode())
    pass_sha1 = encrypted_password_digest.hexdigest()

    # Computes the Password Digest
    pass_digest = base64.b64encode(pass_sha1.encode()).decode()

    headers= {
    'Content-Type': 'application/json',
    'X-WSSE': 'UsernameToken Username="{}", PasswordDigest="{}", Nonce="{}", Created="{}"'.format(
        username,
        pass_digest,
        nonce,
        timestamp)}
    return headers
def lambda_handler(event, context):
    # responseId="5c93a1de-5543-41d4-a0a6-fb8297bcc32d"
    print("event:",event)
    responseId=event["responseId"]
    opco=event["opco"]
    print("responseId:",responseId)
  
    response = table.scan(
        FilterExpression='responseId = :responseId and opco = :opco',
        ExpressionAttributeValues={':responseId': responseId, ':opco': opco}
    )
    
    if response['Count'] == 0:
        return {
            'statusCode': 404,
            'body': json.dumps('ResponseId not found in database')
        }
    toAddress = []
    delivery_status = []
    
    for item in response['Items']:
        toAddress.append(item.get('toAddress')) 
        
        if 'delivery_status' in item:
            delivery_status.append(item['delivery_status'])
        else:
            delivery_status.append(None)
    
    print("delivery_status:", delivery_status)
    print("toAddress:", toAddress)
    # toAddress = [item['toAddress'] for item in response['Items']]
    # delivery_status = [item['delivery_status'] for item in response['Items']]
    # print("delivery_status:",delivery_status)
    # print("toAddress:",toAddress)
    #######################create contact list ###################
    # #API endpoint 
    url_create_list = 'https://api.emarsys.net/api/v2/contactlist'
    mapped_item = {
        "key_id": "15",
        "name": f"{responseId}_feedback",
        "description": "",
        "external_ids": toAddress
    }
    print("create new contact list payload:",mapped_item)
    response_create_list = requests.post(url_create_list, headers=get_auth(username, secret), data=json.dumps(mapped_item)).json()
    print("create new contact list response:",response_create_list)
    # print("response_create_list",response_create_list)  
    
   
    # response_create_list= {'replyCode': 0, 'replyText': 'OK', 'data': {'id': 1928097223}} ##new contact
    # response_create_list = {'replyCode': 3005, 'replyText': 'Contact list with the requested name already exists.', 'data': ''} 
    #####################get contact list#########
    if response_create_list['data'] == '':
        contactlist="https://api.emarsys.net/api/v2/contactlist"
        response_contactlist = requests.get(contactlist, headers=get_auth(username, secret))
        # print(response_contactlist.text)
        contact_list = response_contactlist.json()
        filtered_contact_list = [item for item in contact_list['data'] if item['name'] == f"{responseId}_feedback"]
        contact_list_id=filtered_contact_list[0]['id']
        print("Getting contact_list_id:",contact_list_id)
     
        #######upadte contact list#####
        url_update_contacts = 'https://api.emarsys.net/api/v2/contact/'
        payload_update = {
            "key_id": "15",
            "contact_list_id": contact_list_id,
            "contacts": [
                {
                    "15": address,
                    "849": delivery_status[i],
                    "827": ""
                } for i, address in enumerate(toAddress)
            ]
        }
        response_update_contacts = requests.put(url_update_contacts, headers=get_auth(username, secret), data=json.dumps(payload_update))
        # print(response_update_contacts)
        print("upadted contactList payload:",payload_update)
        print("old_update_contacts:",response_update_contacts.json())
    else:
        if response_create_list['data'] != '':
            url_update_contacts = 'https://api.emarsys.net/api/v2/contact/'
            payload_update = {
                "key_id": "15",
                "contact_list_id": response_create_list['data']['id'],
                "contacts": [
                    {
                        "15": address,
                        "849": delivery_status[i],
                        "827": ""
                    } for i, address in enumerate(toAddress)
                ]
            }
            response_update_contacts = requests.put(url_update_contacts, headers=get_auth(username, secret), data=json.dumps(payload_update))
            # print(response_update_contacts)
            print("upadted contactList payload:",payload_update)
            print("new_response_update_contacts",response_update_contacts.json())
        

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


