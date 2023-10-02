1. Preparing GCP Iceberg Classes

   Download and build https://github.com/rafaelbey/iceberg/tree/biglake

   `BigLakeCatalog` is not available yet from the official Iceberg project.
   
   This fork include the changes from PR: https://github.com/apache/iceberg/pull/7412 while also adding a mechanisms to directly provide an access token.


2. GCP requirements
   1. Service account with Storage Admin, BigQuery Admin, and BigLake Admin roles
      - We will use on the steps below `gstest@sky-225649-proj-c89517cd.iam.gserviceaccount.com`
   2. Parquet files already exists in a gs bucket
      - This uses parquet files created thru external managed BigQuery tables
      - We will use the following parquet files:
        - `gs://gstest-bucket-1/biglake1/data/b409209b-2554-4e3f-86ee-adda00f26796-296421583d0f44b0-f-00000-of-00001.parquet`
        - `gs://gstest-bucket-1/biglake1/data/d53c5a4d-9cc2-431c-8958-5a6428b5fd12-6f08412c7a183267-f-00000-of-00001.parquet`
   3. More information on GCP setup https://github.com/epsstan/legend-engine/blob/3acb086705e7437d08215de1afd29e811993d518/gcp_iceberg/README.md

2. Prepare Test Code
   1. Sample maven for the respective dependencies
   2. Main class for connecting to GCP, accessing BLMS, and creating BLMS table

3. Getting an Access Token
   1. Go to https://console.cloud.google.com/
   2. Open a terminal (Activate Cloud Shell)
   3. Request token:
      
      `curl -X POST -H "Authorization: Bearer "$(gcloud auth application-default print-access-token) -H "Content-Type: application/json; charset=utf-8" -d @request.json "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/gstest@sky-225649-proj-c89517cd.iam.gserviceaccount.com:generateAccessToken"`
   4. Copy the response json and pasted into the test code:
      
      `String accessTokenJson = "___ACCESS_TOKEN_JSON___";`

4. Run test code

5. Create BLMS external table in BigQuery Console

   ```sql
   CREATE EXTERNAL TABLE `test_dataset.rafael_test3_external` WITH CONNECTION `us.bq_connection`
   OPTIONS (format='ICEBERG', uris=['blms://projects/sky-225649-proj-c89517cd/locations/us/catalogs/iceberg_catalog/databases/iceberg_warehouse/tables/rafael_test3'])
   ```
   
6. Query table in BigQuery Console

   ```sql
   SELECT * FROM `sky-225649-proj-c89517cd.test_dataset.rafael_test3_external`
   ```