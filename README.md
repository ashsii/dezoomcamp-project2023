# dezoomcamp-project2023

1. create dezoomcamp-project2023
    - set custom project id (crypto-groove-382408)
2. setup service account de-user
3. give user permissions
4. download sdk for local setup
5. set keys
6. enable apis

just main.tf and variables.tf needed
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"; 
 
terraform init
terraform plan -var="project="
terraform apply -var="project="

pip install -r requirements.txt
prefect orion start
prefect block register -m prefect_gcp
Create a GCP Credentials block in the UI.

zoom-gcp-creds : credentials
zoom-gcs : bucket


python code.py to run prefect flow

or

build deployments

$ prefect deployment build ./2_etl_gcs_to_bq.py:etl_parent_flow -n "Parameterized ETL"

$ prefect deployment apply etl_parent_flow-deployment.yaml

start agent 
prefect agent start -q 'default'

use custom run and enter custom parameters

requires parameters {"project_id":"project_id", "dataset_name":"letterboxd_data"}


https://lookerstudio.google.com/s/h9nNKkspcgk

add column year
YEAR(release_date)
average_rating>8
