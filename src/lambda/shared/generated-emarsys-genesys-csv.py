import boto3
import pandas as pd
from datetime import datetime, timedelta, date
import awswrangler as wr
import json
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('emarsys-genesys-join-db')

current_day = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
current_day_year = current_day[0:4]
current_day_month = current_day[4:6]
current_day_day = current_day[6:]

date_string = "{}{}{}".format(current_day_year, current_day_month, current_day_day)

def lambda_handler(event, context):
    try:
        # Filter at DynamoDB level to avoid loading entire table into memory
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=3)
        filter_expr = Attr('Date').gte(str(start_date)) & Attr('Date').lte(str(end_date))

        response = table.scan(FilterExpression=filter_expr)
        items = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], FilterExpression=filter_expr)
            items.extend(response['Items'])

        df = pd.DataFrame(items)
        df['Date'] = pd.to_datetime(df['Date'])

        # Filter based on the last 4 days
        date_filter = (df['Date'].dt.date >= start_date) & (df['Date'].dt.date <= end_date)
        filtered_df = df[date_filter].copy()
        print("step1:", filtered_df.columns)

        # Check if required columns are present, if not, add them
        required_columns = ["INTERACTION_AGENT_NAME", "INTERACTION_START_TIME", "INTERACTION_ID",
                            "SOURCE_SYSTEM_NAME", "INTERACTION_CHANNEL", "INTERACTION_END_TIME"]
        missing_columns = set(required_columns) - set(filtered_df.columns)

        if missing_columns:
            for column in missing_columns:
                filtered_df[column] = None  # Add missing columns with None values
        print("step2:", filtered_df.columns)

        # Dropping delivery_status = 'empty'
        print(filtered_df.Delivery_Status.unique())
        print('len before:', len(filtered_df))
        
        filtered_df['Delivery_Status'] = filtered_df['Delivery_Status'].astype(str)
        filtered_df['Delivery_Status'] = filtered_df['Delivery_Status'].replace('nan', 'failed')
        filtered_df_unique = filtered_df.copy()
        
        print(filtered_df_unique.Delivery_Status.unique())
      
        # Update DELIVERY_EVENT_DESCRIPTION
        filtered_df_unique['DELIVERY_EVENT_DESCRIPTION'] = filtered_df_unique['DELIVERY_EVENT_DESCRIPTION'].replace(
            'Message delivery failed with the following payload: [GeneralError - Rate limit hit]',
            'Message delivery failed with the following payload: [GeneralError - Message Undeliverable.]'
        )
        
        # Add sequence numbers directly
        filtered_df_unique['sequence'] = filtered_df_unique.groupby('campaign_name').cumcount() + 1
        
        # Storing the whole file
        backup_path = f"s3://lla-emarsys-mapping/backupfile/emarsys-outbound-report-{date_string}.csv"
        wr.s3.to_csv(df=filtered_df_unique, path=backup_path, index=False)
        
        filtered_df_unique = filtered_df_unique.drop(['conversationId', 'msg_id'], axis=1, errors='ignore')


        # Group the filtered DataFrame by the 'market' column
        grouped = filtered_df_unique.groupby('market')

        for market, market_df in grouped:
            if market == 'LCPR':
                bucket_path = 'Outbound_Campaign_Report/LCPR'
            elif market == 'BHS':
                bucket_path = 'Outbound_Campaign_Report/BHS'
            elif market == 'BRB':
                bucket_path = 'Outbound_Campaign_Report/BRB'
            elif market == 'JAM':
                bucket_path = 'Outbound_Campaign_Report/JAM'
            elif market == 'CUW':
                bucket_path = 'Outbound_Campaign_Report/CUW'
            elif market == 'TTO':
                bucket_path = 'Outbound_Campaign_Report/TTO'
            elif market == 'CYM':
                bucket_path = 'Outbound_Campaign_Report/CYM'
            elif market == 'CWP':
                bucket_path = 'Outbound_Campaign_Report/CWP'
            else:
                print("Invalid Market Found")
                continue
            # Write the final data to the lla-emarsys-mapping bucket
            emarsys_bucket_name = 'lla-emarsys-mapping'
            emarsys_file_path = f"s3://{emarsys_bucket_name}/{bucket_path}/Outbound_Campaign_Report-{date_string}.csv"
            wr.s3.to_csv(df=market_df, path=emarsys_file_path, index=False)
            print(f"Emarsys mapping file written for {market}")
            
            # Write the same data to the polaris-export bucket
            polaris_bucket_name = 'polaris-export'
            polaris_file_path = f"s3://{polaris_bucket_name}/{bucket_path}/Outbound_Campaign_Report-{date_string}.csv"
            wr.s3.to_csv(df=market_df, path=polaris_file_path, index=False)
            print(f"Export bucket file written for {market}")

            # # Delete data from DynamoDB based on partitionKey and sortKey
            # for _, row in filtered_df.iterrows():
            #     toAddress = row['toAddress']
            #     Date = str(row['Date'])  # Convert to string
            #     table.delete_item(
            #         Key={
            #             'toAddress': toAddress,
            #             'Date': Date
            #         }
            #     )

            # print("Data deleted from DynamoDB")
    except Exception as e:
        print("Outbound report file generating have exception ",e)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn='arn:aws:sns:us-east-1:030615279213:EXCEPTION_MESSAGE',
            Message=f'Outbound report file generation has an exception. ,error : ' + str(e),
            Subject='!!! generated-emarsys-genesys-csv : file-generating exception!!!',
        )
