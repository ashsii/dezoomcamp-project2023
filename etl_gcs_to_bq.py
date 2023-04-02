from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(data: str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{data}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    return Path(f"data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(len(df))
    print(df.dtypes)
    return df

@task()
def create_bq_table(projectId:str,dataset:str, table:str) -> None:
    # Load gcp bq client
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    client = gcp_credentials_block.get_bigquery_client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"{projectId}.{dataset}.{table}"

    # Set Schema based on table
    if table == 'movies':
        schema = [
            bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("genres", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("image_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("imdb_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("imdb_link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("movie_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("movie_title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("original_language", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("overview", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("popularity", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("production_countries", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("release_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("runtime", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("spoken_languages", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tmdb_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tmdb_link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("vote_average", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("vote_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("year_released", "INT64", mode="NULLABLE"),
        ]
    elif table == 'ratings':
        schema = [
            bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("movie_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rating_val", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
        ]
    elif table == 'users':
        schema = [
            bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("num_ratings_pages", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("num_reviews", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("username", "STRING", mode="NULLABLE"),
        ]

    # Create table
    table = bigquery.Table(table_id, schema=schema)
    try:
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    except:
        print("Table already exists")


@task()
def write_bq(df: pd.DataFrame, projectId:str, dataset:str, table:str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table=f"{dataset}.{table}",
        project_id=f"{projectId}",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    project_id = 'crypto-groove-382408'
    dataset_name = 'letterboxd_data'

    create_bq_table(project_id, dataset_name,'movies') 
    create_bq_table(project_id,dataset_name,'ratings') 
    create_bq_table(project_id,dataset_name,'users') 

    path_movie = extract_from_gcs('movie_data')
    df_movie = transform(path_movie)
    path_ratings = extract_from_gcs('ratings_data')
    df_ratings = transform(path_ratings)
    path_users = extract_from_gcs('users_data')
    df_users = transform(path_users)

    write_bq(df_movie, project_id,dataset_name,'movies') 
    write_bq(df_ratings, project_id,dataset_name,'ratings') 
    write_bq(df_users, project_id,dataset_name,'users') 

if __name__ == "__main__":
    etl_gcs_to_bq()
