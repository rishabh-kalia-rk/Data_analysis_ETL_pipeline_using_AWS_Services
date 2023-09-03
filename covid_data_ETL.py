

import boto3
import pandas as pd
import redshift_connector
from io import StringIO
import time
import configparser

import configparser
config= configparser.ConfigParser()
config.read_file(open('cluster2.config'))

AWS_ACCESS_KEY =config.get('AWS','KEY')
AWS_SECRET_KEY =config.get('AWS','SECRET')
AWS_REGION=config.get('AWS','REGION')
SCEHMA_NAME=config.get('DWH','SCHEMA_NAME')
S3_STAGING_DIR=config.get('DWH','S3_STAGING_DIR')
S3_BUCKET_NAME=config.get('DWH','S3_BUCKET_NAME')
S3_OUTPUT_DIRECTORY=config.get('DWH','S3_OUTPUT_DIRECTORY')
S3_FINAL_BUCKET_NAME=config.get('DWH','S3_FINAL_BUCKET_NAME')
S3_OUTPUT_TABLE_DATA=config.get('DWH','S3_OUTPUT_TABLE_DATA')
REDSHIFT_DB_NMAE = config.get('DWH','REDSHIFT_DB_NMAE')
REDSHIFT_USER    = config.get('DWH','REDSHIFT_USER')
REDSHIFT_PASS     = config.get('DWH','REDSHIFT_PASS')
REDSHIFT_ENDPOINT = config.get('DWH','REDSHIFT_ENDPOINT')
REDSHIFT_S3_IAM=config.get('DWH','REDSHIFT_S3_IAM')

athena_client=boto3.client(
"athena",
aws_access_key_id=AWS_ACCESS_KEY,
aws_secret_access_key=AWS_SECRET_KEY,
region_name=AWS_REGION

)

# this function takes the result of query that we run in athena and download the result and return dataframe of that result.

Dict={}
def download_and_load_query_results(
    client: boto3.client, query_response: Dict, x: str):
    while True:
        try:
            #this function only loads the first 1000 rows
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    temp_file_location : str = f"./athena_query_results_{x}.csv"
    s3_client = boto3.client(
            "s3",
             aws_access_key_id=AWS_ACCESS_KEY,
             aws_secret_access_key=AWS_SECRET_KEY,
             region_name=AWS_REGION,)
    s3_client.download_file(   
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location
    )
    return pd.read_csv(temp_file_location)


# Now we will be processing different table and apply query. Then return the dataframe of them.
query = "enigma_jhud"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)


enigma_jhud= download_and_load_query_results(athena_client, response, query)

query="nytimes_data_in_usa_us_county"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)


nytimes_data_in_usa_us_county= download_and_load_query_results(athena_client, response, query)

query="nytimes_data_in_usa_us_states"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)

nytimes_data_in_usa_us_states= download_and_load_query_results(athena_client, response, query)



query="static_datacountrycode"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)


static_datacountrycode= download_and_load_query_results(athena_client, response, query)

query="static_datacountypopulation"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)


static_datacountypopulation= download_and_load_query_results(athena_client, response, query)

query="rearc_covid_19_testing_dataus_daily"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)


rearc_covid_19_testing_dataus_daily= download_and_load_query_results(athena_client, response,query)

query="rearc_covid_19_testing_data_states_dailystates_daily"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)


rearc_covid_19_testing_data_states_dailystates_daily= download_and_load_query_results(athena_client, response,query)

query="rearc_usa_hospital_beds"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)


rearc_usa_hospital_beds= download_and_load_query_results(athena_client, response,query)

query="rearc_covid_19_testing_dataus_total_latest"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)


rearc_covid_19_testing_dataus_total_latest= download_and_load_query_results(athena_client, response,query)

query="static_datastate_abv"
response = athena_client.start_query_execution(
    QueryString=f"SELECT * FROM {query}",
    QueryExecutionContext={"Database":SCEHMA_NAME},
    ResultConfiguration={
        "OutputLocation":S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
    },
)


static_datastate_abv= download_and_load_query_results(athena_client, response, query)



# to resolve the issue in static_dataset_abv, ablut column name.
new_header= static_datastate_abv.iloc[0] # to grab first row fro header, iloc is a function in pandas.
static_datastate_abv=static_datastate_abv[1:] # take the data except the header row
static_datastate_abv.columns= new_header # set the header 


# Now to merge the table to create the dimensional model.

factCovid_1= enigma_jhud[['fips','province_state','country_region','confirmed','deaths','recovered','active']]
factCovid_2= rearc_covid_19_testing_data_states_dailystates_daily[['fips','date','positive','negative','hospitalizedcurrently','hospitalized','hospitalizeddischarged']]
factCovid=pd.merge(factCovid_1,factCovid_2, on='fips',how='inner')
dimRegion_1 = enigma_jhud[['fips','province_state','country_region', 'latitude','longitude']]


dimRegion_2= nytimes_data_in_usa_us_county[['fips','county','state']]
dimRegion=pd.merge(dimRegion_1,dimRegion_2,on='fips',how='inner')

dimHospital=rearc_usa_hospital_beds[['fips','state_name','latitude','longtitude','hq_address','hospital_type','hq_city','hq_state']]

dimDate=rearc_covid_19_testing_data_states_dailystates_daily[['fips','date']]

dimDate['date']=pd.to_datetime(dimDate['date'],format="%Y%m%d")

# to change the formating of the date present in date columna nd create different column like or year, month.
dimDate['year']=dimDate['date'].dt.year
dimDate['month']=dimDate['date'].dt.month
dimDate['day_of_week']=dimDate['date'].dt.dayofweek



csv_buffer=StringIO()
factCovid.to_csv(csv_buffer)
s3_resource=boto3.resource('s3',region_name='ap-south-1',aws_access_key_id= AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY )
s3_resource.Object(S3_FINAL_BUCKET_NAME,'output/factCovid.csv').put(Body=csv_buffer.getvalue())



dimDate.to_csv(csv_buffer)
s3_resource=boto3.resource('s3',region_name='ap-south-1',aws_access_key_id= AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY )
s3_resource.Object(S3_FINAL_BUCKET_NAME,'output/dimDate.csv').put(Body=csv_buffer.getvalue())

csv_buffer= StringIO()
dimHospital.to_csv(csv_buffer)
s3_resource=boto3.resource('s3',region_name='ap-south-1',aws_access_key_id= AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY )
s3_resource.Object(S3_FINAL_BUCKET_NAME,'output/dimHospital.csv').put(Body=csv_buffer.getvalue())

csv_buffer= StringIO()
dimRegion.to_csv(csv_buffer)
s3_resource=boto3.resouce('s3')
s3_resource.Object(S3_FINAL_BUCKET_NAME,'output/dimRegion.csv').put(Body=csv_buffer.getvalue())




conn=redshift_connector.connect(
    host=REDSHIFT_ENDPOINT,
    database=REDSHIFT_DB_NMAE,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASS)
    


conn.autocomit =True

cursor= redshift_connector.Cursor= conn.cursor()


# To create table in redshift database.
cursor.execute(""""
CREATE TABLE "dimDate"(
    "index" INTEGER,
    "fips" INTEGER,
    "date" TIMESTAMP,
    "year" INTEGER,
    "month" INTEGER,
    "day_of_week" INTEGER
)
""")

cursor.execute(""""
CREATE TABLE "dimHospital"(
    "index" INTEGER,
    "fips" REAL,
    "state_name" TEXT,
    "latitude" REAL,
    "longtitude" REAL,
    "hq_address" TEXT,
    "hospital_name" TEXT,
    "hospital_type" TEXT,
    "hq_city" TEXT,
    "hq_state" TEXT
)
""")
cursor.execute(""""
CREATE TABLE "factCovid"(
    "index" INTEGER,
    "fips" REAL,
    "provience_state"   TEXT,
    "country_region" TEXT,
    "confirmed" REAL,
    "death" REAL,
    "recovered" REAL,
    "active" REAL,
    "date" INTEGER,
    "positive" REAL,
    "negative" REAL,
    "hospitalizedcurrently" REAL,
    "hospitalized" REAL,
    "hospitalizeddischarged" REAL
)
""")

cursor.execute("""CREATE TABLE "dimRegion" (
"index" INTEGER,
"flips" REAL,
"province_satte" TEXT,
"country_region" TEXT,
"latitude" REAL,
longitude" REAL,
"country" TEXT,
"satte" TEXT
)""")



# to copy data from S3 bucket to tables above created in redshift database. 

cursor.execute(
"""
copy dimDate from %s
credentials 'aws_iam_role=%s
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""",(f'{S3_OUTPUT_TABLE_DATA}/dimDate.csv',REDSHIFT_S3_IAM,)
)

cursor.execute(
"""
copy dimHospital from %s
credentials 'aws_iam_role=%s
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""",(f'{S3_OUTPUT_TABLE_DATA}/dimHospital.csv',REDSHIFT_S3_IAM,)
)


cursor.execute(
"""
copy dimRegion from %s
credentials 'aws_iam_role=%s
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""",(f'{S3_OUTPUT_TABLE_DATA}/dimRegion.csv',REDSHIFT_S3_IAM,)
)

cursor.execute(
"""
copy factCovid from %s
credentials 'aws_iam_role=%s
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""",(f'{S3_OUTPUT_TABLE_DATA}/factCovid.csv',REDSHIFT_S3_IAM,)
)

# NOw the data is loaded in the tables present in the redshift database we can apply the quires on them and can process the data further for analysis.