# Data Engineer Zoomcamp Capstone Project

Data based on https://www.kaggle.com/datasets/samlearner/letterboxd-movie-ratings-data

Download the data and place in /data/ in the same directory.

On letterboxd movies are rated out of 5 stars. The goal of this project is to see how the ratings of movies change over time.

## Has the ratings of movies over time gotten worse or better?

### Requirements: 
- Google Account
    For Google Data studio used for dashboards
- Google Cloud Platform Account
    - Will use Storage Buckets for Data Lake and Bigquery for Data Warehouse
- Local System with:
    - GCP SDK: Connects local to GCP
    - Python: For transforming data
    - Terraform: Infrastructure-as-Code (IaC)
    - Prefect: flow orchestration of pipeline
        - (installed by requirements.txt)

   

### Instructions 
#### Setup GCP Project
1. Create project dezoomcamp-project2023 on Google Cloud Platform
    - Set a custom project id
2. Setup Service Account 'de-user' via the IAM-Admin Service Accounts page
![image](https://user-images.githubusercontent.com/19361148/230008035-9e8197d2-4b3d-47df-a746-55d80fa873cc.png)
3. Create keys and export json
![image](https://user-images.githubusercontent.com/19361148/230008502-de79062f-8dab-44b1-810c-d61d6e92e178.png)
4. Give Service Account Permissions
![image](https://user-images.githubusercontent.com/19361148/230008173-7860cecc-4c06-4285-9f05-81990848e612.png)
![image](https://user-images.githubusercontent.com/19361148/230008322-7db92c61-c8b0-465a-ae0c-9336535560fb.png)
5. Enable apis
    - https://console.cloud.google.com/apis/library/iam.googleapis.com
    - https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
6. Set keys locally
    - `export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json";` 

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

Build deployments of parametized flows

`prefect deployment build ./2_flows/2_etl_gcs_to_bq.py:etl_parent_flow -n "Parameterized ETL"`

Requires parameters {"project_id":"project_id", "dataset_name":"letterboxd_data"}

`prefect deployment apply etl_parent_flow-deployment.yaml`

Start agent 

`prefect agent start -q 'default'`

Use custom run and enter custom parameters

#### Data Studio
Log into data studio and create dashboard:

https://lookerstudio.google.com/s/h9nNKkspcgk

Custom fields based on:

`YEAR(release_date)`

`average_rating>8`

![image](https://user-images.githubusercontent.com/19361148/230004974-e530e131-478f-445e-b907-9829bbf11423.png)

