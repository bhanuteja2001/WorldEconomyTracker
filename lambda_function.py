import pandas as pd
import requests
import datetime
import os
import boto3
import pg8000
import json

os_input_s3_name = os.environ['s3_name']
current_date = datetime.date.today()
redshift_config = {
        'host': os.environ('REDSHIFT_HOST'),
        'port': int(os.environ('REDSHIFT_PORT')),
        'database': os.environ('REDSHIFT_DATABASE'),
        'user': os.environ('REDSHIFT_USER'),
        'password': os.environ('REDSHIFT_PASSWORD'),
        'table_name': os.environ('REDSHIFT_TABLE')
    }
def insert_into_redshift(json_data, redshift_config):
    conn = pg8000.connect(
        host=redshift_config['host'],
        port=redshift_config['port'],
        database=redshift_config['database'],
        user=redshift_config['user'],
        password=redshift_config['password']
    )

    cursor = conn.cursor()
    insert_query = """
    INSERT INTO {table_name} (json_data) VALUES (%s);
    """.format(table_name=redshift_config['table_name'])
    
    # Convert JSON data to JSON string
    json_string = json.dumps(json_data)
    
    # Execute SQL query
    cursor.execute(insert_query, (json_string,))
    
    # Commit the transaction
    conn.commit()
    
    # Close the connection
    conn.close()
    
    return "Data inserted successfully"





def lambda_handler(event, context):
    response_exchange = requests.get("http://api.nbp.pl/api/exchangerates/tables/A/{}".format(current_date))
    print("Status code for exchange table", response_exchange.status_code)
    response_gold = requests.get("http://api.nbp.pl/api/cenyzlota/{}".format(current_date))
    print("Status code for gold table", response_gold.status_code)

    s3 = boto3.client('s3')

    if response_exchange.status_code == 200 and response_gold.status_code == 200:
        row = {"Date" : current_date}
        gold_price = response_gold.json()[0]["cena"]
        
        for currency in range(len(response_exchange.json()[0]["rates"])):
            row_key = response_exchange.json()[0]["rates"][currency]["code"]
            row_value = response_exchange.json()[0]["rates"][currency]["mid"]
            row[str(row_key)] = row_value 
        
        file_name = "{}.json".format(current_date)
        row["gold_price"] = gold_price

        s3.put_object(Body=row, Bucket=os_input_s3_name, key=file_name)

        #writing to Redshift
        status = insert_into_redshift(row, redshift_config)
    
        return {
            'statusCode': 200,
            'body': json.dumps(status)
        }

    else:
        print(response_exchange.status_code, response_gold.status_code)