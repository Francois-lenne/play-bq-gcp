# Library use for the project

from psnawp_api import PSNAWP
import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq

# Create the connection with the psn API using the psnawp_api library

psnawp = PSNAWP(os.getenv('psn'))

client = psnawp.me()

print(client)


# Get the trophee data into a pandas dataframe


profile = client.get_profile_legacy()
trophee = profile["profile"]["trophySummary"]["earnedTrophies"]

print(trophee)


## Convert the dictionnary into a pandas dataframe

df_trophee = pd.DataFrame.from_dict(trophee, orient='index').T

### add a column withe the date of execution with the format YYYY-MM-DD

df_trophee['date'] = pd.Timestamp.now().date()






# Get the data of the playsation user

## initialize the class in order to retrieve the games played by the user and is stats

titles_with_stats = client.title_stats()

print(titles_with_stats)

## transfrom the data into a pandas dataframe to facilitate the laod in BigQuery 

import pandas as pd

# Supposons que TitleStatsListing a un attribut 'titles' qui est une liste d'objets TitleStats
data = [[ts.title_id, ts.name, ts.image_url, ts.category, ts.first_played_date_time, ts.last_played_date_time , ts.play_count, ts.play_duration] for ts in titles_with_stats]

# Convertir les données en un DataFrame
df = pd.DataFrame(data, columns=['title_id', 'title_name', 'image', 'category', 'first_played_date_time','last_played_date_time' ,'play_count', 'play_duration'])




# Load the data into BigQuery

## Create the dataset in BigQuery


## retreive the credentials with a environnement variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/francoislenne/Downloads/united-triode-379809-9e9e13a60ff7.json"

## initialise the client
client = bigquery.Client()

## setup the dataset name
dataset_id = "psn"

# Créez un objet DatasetReference en utilisant le dataset_id
dataset_ref = client.dataset(dataset_id)

# Créez un objet Dataset à partir de l'objet DatasetReference
dataset = bigquery.Dataset(dataset_ref)

# Spécifiez les paramètres du dataset
dataset.location = "EU" 

## Create the dataset
dataset = client.create_dataset(dataset)  # API request

print("Dataset créé. {}".format(dataset.full_dataset_id))







## Load the data trophee in BigQuery

import pandas_gbq

### Define the project ID and dataset ID
project_id = "united-triode-379809"
dataset_id = "psn"

### Define the table ID
table_id = "trophee-earned"


### Convert the date columns to datetime for BigQuery
object_cols = df_trophee.select_dtypes(include=['object']).columns

for column in object_cols:
    dtype = str(type(df_trophee[column].values[0]))
    if dtype == "<class 'datetime.date'>":
        df_trophee[column]  = pd.to_datetime(df_trophee[column] , infer_datetime_format=True)

### Load the dataframe into BigQuery
pandas_gbq.to_gbq(df_trophee, f"{project_id}.{dataset_id}.{table_id}", project_id=project_id, if_exists="replace")




## Load the games data in Bigquery

### Modification of the column in order to be able to load the data in BigQuery
df['title_id'] = df['title_id'].str.replace('_', '')
df['category'] = df['category'].astype(str).str[-3]


### Convert the date columns to datetime for BigQuery


object_cols = df.select_dtypes(include=['object']).columns

for column in object_cols:
    dtype = str(type(df[column].values[0]))
    if dtype == "<class 'datetime.date'>":
        df[column]  = pd.to_datetime(df[column] , infer_datetime_format=True)


### Convert the time delta columns to timedelta for BigQuery
for column in df.select_dtypes(include=['timedelta64[ns]']).columns:
    df[column] = df[column].dt.total_seconds()

### Load the dataframe into BigQuery
        
pandas_gbq.to_gbq(df, f"{project_id}.{dataset_id}.{table_id}", project_id=project_id, if_exists="replace")