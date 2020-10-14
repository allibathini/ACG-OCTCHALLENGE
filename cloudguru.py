import pandas as pd
from datetime import datetime
import sys
#from data_transformation import *
import boto3
import base64
from botocore.exceptions import ClientError
import psycopg2

from cloudguru.data_transformation import df_data_transform


def reading_csv_files(dataset_URL, DATASET_NAME):
    try :
        res = pd.read_csv(dataset_URL,header=0, delimiter=',',index_col = False)
        return res

    except:
        print("Error while reading CSV data:" + DATASET_NAME + "from Git" )
        sys.exit()

'''
def get_secret():
    secret_name = "rds_postgres"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name, aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':

            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':

            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':

            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':

            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':

            raise e
    else:

        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret
'''
def lambda_handler(event, context):
# Variables Initialization
 NYTset = 'NYT DATASET'
 JohnHOpkinsset = 'JohnHopkins DATASET'
 NYTdataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
 JohnHopkinsURL = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1602173839233r0.3109590658815089'
 ACCESS_KEY = 'AKIAYLWKPTTEIRSGDKZ7'
 SECRET_KEY = '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'
 table_name = 'covid19_table_test'
 DT = datetime.now().strftime('%Y-%m-%d')


# ------ Reading CSV Files ------

############ REading FIles #######

 NYTData =reading_csv_files(NYTdataURL,NYTset)
 JOhnHopkinsData = reading_csv_files(JohnHopkinsURL, JohnHOpkinsset)
 print('Establishing connection to to RDS Postgres Database')

#Postgres_conn = psycopg2.connect("host=covid19-challenge.chmkxpe5o969.us-west-2.rds.amazonaws.com dbname=covid19_test user=postgres password=abcd1234 port=5432")
 Postgres_conn =psycopg2.connect(dbname='covid19_test', user='postgres', password='abcd1234', host='covid19-challenge.chmkxpe5o969.us-west-2.rds.amazonaws.com',port=5432)



#table_check_query = "select to_regclass('" + table_name + "')"
 table_check_query = "select to_regclass('" + table_name + "')"
#table_check_query = "select 1 from " + table_name
#print(table_check_query)
 cursor = Postgres_conn.cursor()
 cursor.execute(table_check_query, ['{}.{}'.format('schema', 'table')])
 table_exists = cursor.fetchall()[0][0]
#print(table_exists)
 if table_exists == 'None':
    table_exists = 'False'
 else:
    table_exists = 'True'

    #cursor.close()
#print(table_exists)
#cursor.execute(table_check_query, ['{}.{}'.format('schema', 'table')])
#table_exists = bool(cursor.fetchone()[0])
#table_exists = cursor.fetchall()[0][0]
#print(table_exists)
 if table_exists == 'False':


    create_table_query = "CREATE TABLE " + table_name + "(Date date PRIMARY KEY, Cases integer,Deaths integer,Recovered integer);"
    #print(create_table_query)
    cursor = Postgres_conn.cursor()
    cursor.execute(create_table_query)
    #print('Created Table')

 cursor = Postgres_conn.cursor()
 record_check_query = "select count(*) from " + table_name
#print(record_check_query)
 cursor.execute(record_check_query)
 rec_count = cursor.fetchall()[0][0]
 print("Number of Records present in the Database:" + str(rec_count))
 if rec_count == 0:
    initial_load_flag = "True"
    max_date = 'None'
 else:
    initial_load_flag = "False"
    #print(initial_load_flag)
    max_date_query = "select max(Date) from " + table_name
    cursor = Postgres_conn.cursor()

    #print(max_date)
    cursor.execute(max_date_query)
    max_date = cursor.fetchall()[0][0]
    #print(max_date)

    max_date = datetime.strftime(max_date,'%Y-%m-%d')
    #print(max_date)
########## Performing Date Transformation #########
#print(NYTData)
 Merged_Dataset = df_data_transform(NYTData,JOhnHopkinsData,'date','inner',initial_load_flag,max_date)
#print(Merged_Dataset)
 Merged_Datset_final = Merged_Dataset.rename(columns={"date": "Date","cases":"Cases","deaths":"Deaths"})


 for index in Merged_Datset_final.index:
    #print(Merged_Datset_final['Date'][index], Merged_Datset_final['Cases'][index])

    query = """INSERT into """+table_name+""" (date, cases, deaths,recovered) values({},{},{},{})""".format(
        "'" + str(datetime.date(Merged_Datset_final['Date'][index])) + "'", Merged_Datset_final['Cases'][index], Merged_Datset_final['Deaths'][index],
        int(Merged_Datset_final['Recovered'][index]))
    #print(query)
    cursor = Postgres_conn.cursor()
    cursor.execute(query)
    Postgres_conn.commit()

 num_updated = len(Merged_Datset_final.index)
 print("Number of Rows Updated from today's (" +DT +")run:" + str(num_updated))










