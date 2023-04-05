# Data Engineer Zoomcamp Capstone Project

## Has the ratings of movies over time gotten worse or better?

### Requirements:
- Google Cloud Platform Account
- Local System with:
    - GCP SDK
    - Python
    - Terraform
    - Prefect (installed by requirements.txt)
    

### Instructions 
#### Setup GCP Project
1. Create project dezoomcamp-project2023 on Google Cloud Platform
    - Set a custom project id
2. Setup Service Account 'de-user' via the IAM-Admin Service Accounts page
3. Give Service Account Permissions
5. Enable apis
6. Set keys locally
    - export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"; 

#### Use terraform to create buckets/big query dataset
Run within 1_terraform directory:

`terraform init`

`terraform plan -var="project=project id here"`

`terraform apply -var="project=project id here"`


#### Install Prefect

`pip install -r requirements.txt`

`prefect orion start`

#### Setup GCP Blocks

`prefect block register -m prefect_gcp`

Create a GCP Credentials block in the UI.
    - credentials block: named 'zoom-gcp-creds'
    - bucket block: named tzoom-gcs

#### Run flows

`python 1_etl_web_to_gcs.py`

`python 2_etl_gcs_to_bq.py {project id here}`

or

Build deployments

`prefect deployment build ./2_flows/2_etl_gcs_to_bq.py:etl_parent_flow -n "Parameterized ETL"`

requires parameters {"project_id":"project_id", "dataset_name":"letterboxd_data"}

`prefect deployment apply etl_parent_flow-deployment.yaml`

Start agent 

`prefect agent start -q 'default'`

use custom run and enter custom parameters

#### Data Studio
Log into data studio and create dashboard:

https://lookerstudio.google.com/s/h9nNKkspcgk

Custom fields based on:

`YEAR(release_date)`

`average_rating>8`
