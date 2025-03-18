# Workflow for Processing and Loading OpenAlex data into Google BigQuery

This repository contains instructions on how to extract and transform OpenAlex data for data analysis with Google BigQuery.

## Requirements

The following packages are required for this workflow.

- [AWS](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- [Python3](https://www.python.org)
  - [gsutil](https://pypi.org/project/gsutil/)


## Download Snapshot

OpenAlex snapshots are available through AWS. Instructions for downloading
can be found here: https://docs.openalex.org/download-all-data/download-to-your-machine.

```bash
$ aws s3 sync 's3://openalex' 'openalex-snapshot' --no-sign-request
```

## Data transformation

To reduce the size of the data stored in BigQuery, some data transformation
is applied to the `works` entity. Data transformation is
carried out on the High Performance Cluster of the 
[GWDG Göttingen](https://gwdg.de/en/hpc/). However, you can also 
use the script on other servers with only minor adjustments. Entities 
like `authors`, `publishers`, `institutions`, `funders` and `sources` 
are not affected by the data transformation step.

```bash
$ sbatch openalex_works_hpc.sh
```

## Uploading Files to Google Bucket

Files can be uploaded to a Google Bucket using `gsutil`. Note that we have
only transformed data in the `works` entity. All other data can be found 
in `openalex-snapshot/data`.

```bash
$ gsutil -m cp -r /scratch/users/haupka/works gs://bigschol
```

## Creating a BigQuery Table

Use `bq load` to create a table in BigQuery with data stored in a 
Google Bucket. Schemas for the tables can be found [here](schemas/).

```bash
$ bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON subugoe-collaborative:openalex.works gs://bigschol/works/*.gz schema_openalex_work.json
```

## Notes

- Following fields are not included in the `works` schema:
`mesh`, `related_works`, `concepts`.
- An additional field `has_abstract` is added during the data 
transformation step that replaces the field `abstract_inverted_index`.
