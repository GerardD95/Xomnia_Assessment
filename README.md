This project deploys a cloud run and cloud sql postgres instance on GCP serving metrics regarding shipping information.

Technology used: 
- spark structured streaming to ingest csv shipping data from a minio bucket into cloud sql postgres 
- flask for serving web-endpoints

## Prerequisites
1. A `.env` file with the following vars: 
    - LOCAL_EXECUTION (TRUE|FALSE)
    - SOURCE_TYPE (database|csv)
    - SOURCE_PATH (/path/to/database or csv folder)
    - TF_VAR_image_tag (The docker image tag for the api. Change it for terraform to detect and act upon)
    - TF_VAR_project_id (google project id)
    - TF_VAR_region (google region to deploy to)
    - TF_VAR_zone (google regional)
    - TF_VAR_cloud_sql_instance_name (up to you)
    - TF_VAR_db_name (up to you)
    - TF_VAR_db_user (up to you)
    - TF_VAR_db_pass (up to you)
    - TF_VAR_home_ip (your public ip)
    - DB_HOST_IP (Only for running locally. You'll receive it after deploying the infra)
2. Docker
3. Taskfile=>3.39.2

## How to run
1. `cd` into the `solution` folder
2. run `task infra:registry_deploy`
3. run `task api:push-api`
4. run `task infra:infra_deploy` 
5. run `task data_processing:run-docker-services`
6. go to `http://localhost:9001` (login: minio, minio123) and create a bucket called `test`, a folder called `shipping_data` and upload the `raw_messages.csv` file into the folder.
7. run `task data_processing:ingest-weather-data-gcp`
8. run `task data_processing:start-stream-processor-cloud`
9. find your google cloud run URL and go to one of the following endpoints:
    - `/get-distinct-ship-count`
    - `/get-avg-speed-by-hour`
    - `/get-max-min-wind-speed`
    - `/get-weather-conditions-along-route`

## To destroy:
- `task infra:destroy_all`
- `task data_processing:stop-docker-services`