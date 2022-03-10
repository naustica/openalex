# Workflow for Processing and Loading OpenAlex snapshots into Google BigQuery

This repository contains instructions on how to extract and transform OpenAlex data for data analysis with Google BigQuery.

## Requirements

- [Python3](https://www.python.org)
  - [gsutil](https://pypi.org/project/gsutil/)


## Download Snapshot

```bash
$ aws s3 sync 's3://openalex' 'openalex-snapshot' --no-sign-request
```

## Data transformation

```bash
$ sbatch openalex_hpc.sh
```

## Uploading Files to Google Bucket

```bash
$ gsutil -m cp -r /scratch/users/haupka/works gs://bigschol
```

## Creating a BigQuery Table

```bash
$ bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON subugoe-collaborative:openalex.works gs://bigschol/works/*.gz schema_openalex_work.json
```
