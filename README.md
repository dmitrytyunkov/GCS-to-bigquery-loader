# GCS-to-bigquery-loader
Script for load files of GCS to bigquery
Run comand:
```python script.py --input gs://path/to/uploaded/file --oauth_file oauth.sample.json --schema schema.sample.json --config config.sample.json --runner DataflowRunner --project PROJECT_ID --temp_location gs://path/to/save/temp/files```
