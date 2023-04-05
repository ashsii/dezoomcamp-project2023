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
def transform(path: Path, movies:bool) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    #if movies:
        #df = df[['_id', 'movie_id', 'movie_title', 'overview', 'popularity', 'release_date', 'runtime']]
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
    if table == 'stg_movies':
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
            bigquery.SchemaField("tmdb_id", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("tmdb_link", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("vote_average", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("vote_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("year_released", "INT64", mode="NULLABLE"),
        ]
    elif table == 'stg_ratings':
        schema = [
            bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("movie_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rating_val", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
        ]
    elif table == 'stg_users':
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
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    except:
         print("Table already exists")

@task()
def transform_bq(project_id:str, dataset:str) -> None:
    # Construct a BigQuery client object.
    # client = bigquery.Client()
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    client = gcp_credentials_block.get_bigquery_client()

    query = f"CREATE OR REPLACE TABLE {dataset}.partitioned_movies (_id STRING, movie_id STRING, movie_title STRING, popularity FLOAT64, release_date DATE, runtime FLOAT64) PARTITION BY DATE_TRUNC(release_date, month) AS (SELECT _id, movie_id, movie_title, popularity, release_date, runtime FROM `{project_id}.{dataset}.stg_movies` WHERE release_date is not null);"
    query_job = client.query(query)  # Make an API request.
    query2 = f"CREATE OR REPLACE TABLE {dataset}.clustered_ratings (_id STRING, movie_id STRING, rating_val INTEGER, user_id STRING) CLUSTER BY user_id AS ( SELECT _id, movie_id, rating_val, user_id FROM `{project_id}.{dataset}.stg_ratings` WHERE release_date is not null);"
    query_job2 = client.query(query2) 
    # print("The query data:")
    # for row in query_job:
    #     # Row values can be accessed by field name or index.
    #     print("name={}, count={}".format(row[0], row["total_people"]))

@task() 
def create_view(project_id:str, dataset:str) -> None:
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    client = gcp_credentials_block.get_bigquery_client()

    view_id = f"{project_id}.{dataset}.view_movieratings"
    source_movies_id = f"{project_id}.{dataset}.partitioned_movies"
    source_ratings_id = f"{project_id}.{dataset}.clustered_ratings"
    view = bigquery.Table(view_id)

    # The source table in this example is created from a CSV file in Google
    # Cloud Storage located at
    # `gs://cloud-samples-data/bigquery/us-states/us-states.csv`. It contains
    # 50 US states, while the view returns only those states with names
    # starting with the letter 'W'.
    view.view_query = f"SELECT m.movie_id, m.movie_title, m.popularity, m.release_date, m.runtime, avg(rating_val) as average_rating FROM {source_movies_id} m inner join {source_ratings_id} r on r.movie_id = m.movie_id group by m.movie_id, m.movie_title, m.popularity, m.release_date, m.runtime having count(rating_val) > 100 order by avg(rating_val) desc;"
    # Make an API request to create the view.
    try:
        view = client.create_table(view)
        print(f"Created {view.table_type}: {str(view.reference)}")
    except:
        print('View already found')

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
def etl_gcs_to_bq(project_id:str, dataset_name:str):
    """Main ETL flow to load data into Big Query"""

    # Create tables
    create_bq_table(project_id, dataset_name,'stg_movies') 
    create_bq_table(project_id, dataset_name,'stg_ratings') 
    create_bq_table(project_id, dataset_name,'stg_users') 

    # Get path
    path_movie = extract_from_gcs('movie_data')
    path_ratings = extract_from_gcs('ratings_data')
    path_users = extract_from_gcs('users_data')

    # transformations
    df_movie = transform(path_movie, True)
    df_ratings = transform(path_ratings, False)
    df_users = transform(path_users, False)

    # Write to staging
    write_bq(df_movie, project_id,dataset_name,'stg_movies') 
    write_bq(df_ratings, project_id,dataset_name,'stg_ratings') 
    write_bq(df_users, project_id,dataset_name,'stg_users') 

    # Clean and transform tables
    transform_bq(project_id, dataset_name)
    
    # Create view
    create_view(project_id, dataset_name) 

@flow()
def etl_parent_flow(
    project_id:str = 'project_id', dataset_name:str = 'letterboxd_data'
):
    etl_gcs_to_bq(project_id, dataset_name)

if __name__ == "__main__":
    project_id = 'crypto-groove-382408'
    dataset_name = 'letterboxd_data'
    etl_gcs_to_bq(project_id,dataset_name)
