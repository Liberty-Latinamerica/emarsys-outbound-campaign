import json
import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import datetime
from datetime import datetime, timedelta, timezone
import awswrangler as wr

dynamodb = boto3.resource('dynamodb')
table_name = 'emarsys-genesys-join-db'
table = dynamodb.Table(table_name)
dynamodb_client = boto3.client('dynamodb', 'us-east-1')
dynamodb = boto3.resource('dynamodb')
genesys_db = dynamodb.Table('emarsys-genesys-join-db')

current_day = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
current_day_year = current_day[0:4]
current_day_month = current_day[4:6]
current_day_day = current_day[6:]
date_string = "{}-{}-{}".format(current_day_year, current_day_month, current_day_day)
yesterday_str = str(date_string)
print("yesterday_date:", yesterday_str)
date_string1 = "dt={}-{}-{}".format(current_day_year, current_day_month, current_day_day)
date_string2 = "{}{}{}".format(current_day_year, current_day_month, current_day_day)

def lambda_handler(event, context):
    try:
        response = table.scan()
        items = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        # Filter for items with yesterday's date
        filtered_items = [item for item in items if yesterday_str in item['Date']]
        toAddress = [item['toAddress'] for item in filtered_items]
        queue = [item['queue'] for item in filtered_items]
        Date = [item['Date'] for item in filtered_items]
        date = [item['Date'].split()[0] for item in filtered_items]

        # Define the S3 path
        object_key = "{}/genesys_intr_dtl_{}.csv".format(date_string1, date_string2)
        path = "s3://lla.polarisivr.stage/bigdata/shared/genesys/intr_dtl/" + object_key
        
        # Read the CSV file using awswrangler
        df_dtl = wr.s3.read_csv(path, keep_default_na=False, dtype=str)

        # Convert the "INTERACTION_PHONE_NO" column to string
        df_dtl["INTERACTION_PHONE_NO"] = df_dtl["INTERACTION_PHONE_NO"].astype(str)
        
        # Convert the "INTERACTION_START_TIME" column to datetime
        df_dtl["INTERACTION_START_TIME"] = pd.to_datetime(df_dtl["INTERACTION_START_TIME"], format='ISO8601')
        df_dtl["date"] = df_dtl["INTERACTION_START_TIME"].dt.date
        df_dtl["date"] = df_dtl["date"].astype(str)
        
        # Extract rows from df_dtl based on 'toAddress' and 'queue' values
        filtered_df = df_dtl[(df_dtl["INTERACTION_PHONE_NO"].isin(toAddress)) & (df_dtl["INTERACTION_AGENT_SKILLGROUP"].isin(queue)) & (df_dtl["date"].isin(date))]

        # Rename the columns
        filtered_df = filtered_df.rename(columns={"INTERACTION_PHONE_NO": "toAddress", "INTERACTION_AGENT_SKILLGROUP": "queue"})
        filtered_df["INTERACTION_START_TIME"] = filtered_df["INTERACTION_START_TIME"].astype(str)
        filtered_df["INTERACTION_END_TIME"] = filtered_df["INTERACTION_END_TIME"].astype(str)
        
        df = filtered_df[["toAddress", "queue", "INTERACTION_ID", "INTERACTION_START_TIME", "INTERACTION_END_TIME", "INTERACTION_CHANNEL", "INTERACTION_AGENT_NAME", "SOURCE_SYSTEM_NAME"]].copy()
        
        df_unique = df.drop_duplicates(subset=["toAddress", "queue"])

        print("done")

        updated_count = 0

        for index, row in df_unique.iterrows():
            partition_key = row['toAddress']
            Queue = row['queue']
            query = 'SELECT * FROM "emarsys-genesys-join-db" WHERE toAddress = {} AND queue = {}'.format(f"'{partition_key}'", f"'{Queue}'")
            customer_response = dynamodb_client.execute_statement(Statement=query, ConsistentRead=True)

            for i in customer_response['Items']:
                if yesterday_str in i['Date']['S']:
                    sort_key = i['Date']['S']
                    Delivery_Status = i.get('Delivery_Status', {}).get('S', None)
                    if Delivery_Status is None or Delivery_Status != "replied":
                        # Update the Delivery_Status to "replied"
                        id = row['INTERACTION_ID']
                        start_time = row['INTERACTION_START_TIME']
                        end_time = row['INTERACTION_END_TIME']
                        channel = row['INTERACTION_CHANNEL']
                        agent_name = row['INTERACTION_AGENT_NAME']
                        source_system = row['SOURCE_SYSTEM_NAME']

                        response = genesys_db.update_item(
                            Key={'toAddress': partition_key, 'Date': sort_key},
                            UpdateExpression="SET INTERACTION_ID = :val1, INTERACTION_START_TIME = :val2, INTERACTION_END_TIME = :val3, INTERACTION_CHANNEL = :val4, INTERACTION_AGENT_NAME = :val5, SOURCE_SYSTEM_NAME = :val6, Delivery_Status = :val7",
                            ExpressionAttributeValues={':val1': id, ':val2': start_time, ':val3': end_time, ':val4': channel, ':val5': agent_name, ':val6': source_system, ':val7': "replied"}
                        )
                        updated_count += 1
                    else:
                        print("Delivery_Status is already 'replied'")

        print(f"Total records updated: {updated_count}")

    except Exception as e:
        print(f"An error occurred: {e}")

    print("done")
