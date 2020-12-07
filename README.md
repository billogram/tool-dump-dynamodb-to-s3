# Dump a DynamoDB table to S3

⚠️ Since November 2020 AWS supports the export [without code](https://aws.amazon.com/blogs/aws/new-export-amazon-dynamodb-table-data-to-data-lake-amazon-s3/) hence there is no really a reason to use this tool. 

A script to perform parallel scan of a AWS DynamoDB table and save its rows as
a JSON dataset in a S3 bucket. The processes to read and write the data are run in
parallel. It's meant to be used for gigabyte-sized tables.

There is no guarantee about consistency of the dataset with regard to writes to the
table during the script work.

The script is designed to be used in a AWS Glue `pythonshell` Job hence doesn't depend
on anything but `boto3`.

⚠️ AWS charges extra for scan operations.

### Requirements

* Python >= 3.6.
* Access to the AWS DynamoDB table and the AWS S3 bucket from terminal.

### Quick start

```bash
git clone https://github.com/billogram/tool-dump-dynamodb-to-s3.git
cd tool-dump-dynamodb-to-s3
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
python3 dump.py --table-name TABLE --s3-bucket BUCKET
```

### Advanced usage

```bash
export AWS_ACCESS_KEY_ID=XXXX AWS_SESSION_TOKEN=XXXXXXXXXXX AWS_DEFAULT_REGION=eu-west-1
python3 dump.py \
    --table-name TABLE \
    --s3-bucket BUCKET \
    --total-segments 4 \
    --s3-prefix "raw/TABLE/test/time=$(date -u +'%Y-%m-%dT%H:%M:%SZ')/" \
    --s3-chunk-size-mb 10 \
    --s3-upload-parallel-factor 4 \
    --log-level debug
```

### TODO

* [ ] Report ETA and progress.
* [ ] Unit test.
* [ ] Option to write to stdout.
* [ ] Output compression.
