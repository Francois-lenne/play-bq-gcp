from psnawp_api import PSNAWP
import os
from google.cloud import bigquery
import pandas as pd



# récupération des variables d'envionnements

os.environ['psn'] = ''
os.environ["PROJECT_ID"] = ""
os.environ["DATASET_NAME"] = ""
os.environ["TABLE_NAME_TROPHEE"] = ""
os.environ["TABLE_NAME_GAME"] = ""  
os.environ["TABLE_NAME_TIME_PLAY"] = ""   
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""



psnawp = PSNAWP(os.getenv('psn'))

client = psnawp.me()

print(client)



profile = client.get_profile_legacy()
trophee = profile["profile"]["trophySummary"]["earnedTrophies"]

print(trophee)


# Convertir le dictionnaire en DataFrame
df_trophee = pd.DataFrame.from_dict(trophee, orient='index').T

# add a column withe the date of execution with the format YYYY-MM-DD

df_trophee['date'] = pd.Timestamp.now().date()

# Afficher le DataFrame
print(df_trophee)



# Convert the date columns to datetime for BigQuery
object_cols = df_trophee.select_dtypes(include=['object']).columns

for column in object_cols:
    dtype = str(type(df_trophee[column].values[0]))
    if dtype == "<class 'datetime.date'>":
        df_trophee[column]  = pd.to_datetime(df_trophee[column] , infer_datetime_format=True)




## retrieve the bigquery information
        




def update_trophee(df_trophee):
    # Récupérez les informations à partir des variables d'environnement
    project_id = os.getenv("PROJECT_ID")
    dataset_name = os.getenv("DATASET_NAME")
    table_name_trophee = os.getenv("TABLE_NAME_TROPHEE")

    # Construisez le nom complet de la table BigQuery
    table_id = f"{project_id}.{dataset_name}.{table_name_trophee}"

    # Insérez les données dans la table BigQuery
    df_trophee.to_gbq(table_id, project_id=project_id, if_exists='append')

    return 'Success!'


update_trophee(df_trophee)






def retrieve_game_data():

    titles_with_stats = client.title_stats()

    data = [[ts.title_id, ts.name, ts.image_url, ts.category, ts.first_played_date_time, ts.last_played_date_time , ts.play_count, ts.play_duration] for ts in titles_with_stats]

    df_game = pd.DataFrame(data, columns=['title_id', 'title_name', 'image', 'category', 'first_played_date_time','last_played_date_time' ,'play_count', 'play_duration'])

    df_game['category'] = df_game['category'].astype(str).str[-3:]
    df_game['title_id'] = df_game['title_id'].astype(str)

    object_cols = df_game.select_dtypes(include=['object']).columns

    for column in object_cols:
        dtype = str(type(df_game[column].values[0]))
        if dtype == "<class 'datetime.date'>":
            df_game[column]  = pd.to_datetime(df_game[column] , infer_datetime_format=True)

    return df_game

df_game = retrieve_game_data()

df_game['title_id'] = df_game['title_id'].astype('str')

df_game['title_id'] = df_game['title_id'].str.replace('_', '')


df_game['id'] = df_game['title_id'].apply(lambda x: x[-7:]) + df_game['first_played_date_time'].dt.strftime('%d%H%Y%m')
df_game['id'].astype(str)


object_cols = df_game.select_dtypes(include=['object']).columns

for column in object_cols:
    dtype = str(type(df_game[column].values[0]))
    if dtype == "<class 'datetime.date'>":
        df_game[column]  = pd.to_datetime(df_game[column] , infer_datetime_format=True)

# Convert the time delta columns to timedelta for BigQuery
for column in df_game.select_dtypes(include=['timedelta64[ns]']).columns:
    df_game[column] = df_game[column].dt.total_seconds()


print(len(df_game))















def retrieve_old_game():
    # Créez un client BigQuery
    client = bigquery.Client(location="EU")
    project_id = os.getenv("PROJECT_ID")
    dataset_name = os.getenv("DATASET_NAME")
    table_name_game = os.getenv("TABLE_NAME_GAME")


    # Définissez votre requête
    query = f"SELECT id,title_name,first_played_date_time, last_played_date_time, play_count, play_duration FROM `{project_id}.{dataset_name}.{table_name_game}`"

    # Exécutez la requête et convertissez le résultat en DataFrame pandas
    query_job = client.query(query)
    old_game_df = query_job.to_dataframe()


    return old_game_df



old_game_df = retrieve_old_game()

print(len(old_game_df))






def update_time_play(old_game_df, df_game):

    merged_df = pd.merge(old_game_df, df_game, on='id', suffixes=('_old', '_new'))
    print(merged_df)
    merged_df['play_count_diff'] = merged_df['play_count_new'] - merged_df['play_count_old']
    print(merged_df)
    merged_df['play_duration_diff'] = merged_df['play_duration_new'] - merged_df['play_duration_old']
    print(merged_df)
    selected_df = merged_df[merged_df['play_count_diff'] > 0][['id', 'play_count_diff', 'play_duration_diff']]
    print(selected_df)
    selected_df['date'] = pd.Timestamp.today().normalize()



    return selected_df


time_play_df = update_time_play(old_game_df, df_game)

print(time_play_df)




def load_df_to_bigquery(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    job.result()  # Attendre que le job se termine

    table = client.get_table(table_id)

    return f"Loaded {len(df)} rows to {table_id}"







load_df_to_bigquery(time_play_df, f"{os.getenv('PROJECT_ID')}.{os.getenv('DATASET_NAME')}.{os.getenv('TABLE_NAME_TIME_PLAY')}")

print(time_play_df)


def game_need_update(time_play_df, df_game):
    df_game_filtered = df_game[df_game['id'].isin(time_play_df['id'])]

    return df_game_filtered

df_game_filtered = game_need_update(time_play_df, df_game)

print(df_game_filtered)








def update_bigquery_table_from_df(df_game_filtered, temp_table_id, target_table_id):
    # Chargez le DataFrame dans une table temporaire
    df_game_filtered.to_gbq(temp_table_id, if_exists='replace')

    client = bigquery.Client()

    # Construisez une requête SQL pour mettre à jour la table d'origine à partir de la table temporaire
    query = f"""
    UPDATE `{target_table_id}` target
    SET target.last_played_date_time = temp.last_played_date_time,
        target.play_count = temp.play_count,
        target.play_duration = temp.play_duration
    FROM `{temp_table_id}` temp
    WHERE target.id = temp.id
    """

    # Exécutez la requête
    client.query(query).result()

    # Supprimez la table temporaire
    client.delete_table(temp_table_id, not_found_ok=True)



target_table_id = f"{os.getenv('PROJECT_ID')}.{os.getenv('DATASET_NAME')}.{os.getenv('TABLE_NAME_GAME')}"


temp_table_id = f"{os.getenv('PROJECT_ID')}.{os.getenv('DATASET_NAME')}.temp_table"


# Utilisez la fonction pour mettre à jour une table BigQuery à partir d'un DataFrame
update_bigquery_table_from_df(df_game_filtered, temp_table_id, target_table_id)