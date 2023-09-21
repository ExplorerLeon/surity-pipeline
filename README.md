# surity-pipeline
Personal repo for surity pipeline development and testing

# biq query

GCP Setup
- Service account not needed
    - steps to do is `gcloud auth login --cred-file=CONFIGURATION_FILE`

```
gcloud auth list

gcloud config set account ACCOUNT
```
- gcloud auth steps check
- `cloud auth application-default login`
- use the docs

check local run https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-python

check run through services


## Using Google Cloud Dataflow Beam Runner:

Cloud Dataflow Runner prerequisites and setup
To use the Cloud Dataflow Runner, you must complete the setup in the Before you begin section of the Cloud Dataflow quickstart for your chosen language.

- [x] Select or create a Google Cloud Platform Console project.
- [ ] Enable billing for your project.
- [ ] Enable the required Google Cloud APIs: Cloud Dataflow, Compute Engine, Stackdriver Logging, Cloud Storage, Cloud - - Storage JSON, and Cloud Resource Manager. You may need to enable additional APIs (such as BigQuery, Cloud Pub/Sub, or Cloud Datastore) if you use them in your pipeline code.
- [x] Authenticate with Google Cloud Platform. **check again when doing in production**
- [x] Install the Google Cloud SDK.
- [x] Create a Cloud Storage bucket.
