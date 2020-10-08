# Dump a DynamoDB table to S3

A script to perform parallel scan of a AWS DynamoDB table and save its rows as
a JSON dataset in a S3 bucket. The processes to read and write the data are run in
parallel. It's ment to be used for gigabyte-sized tables.

### Requirements

* Python >= 3.6.
* Access to the AWS DynamoDB table and the AWS S3 bucket from terminal.

### Quick start

```
git clone github.com/billogram/dump-dynamodb-to-s3
cd dump_dynamodb_to_s3
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
python3 dump.py --table-name TABLE --s3-bucket BUCKET
```

### Advanced usage

```
python3 dump.py \
    --table-name TABLE \
    --s3-bucket BUCKET \
    --total-segments 4 \
    --s3-prefix 'raw/dynampdb/TABLE/run_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")/' \
    --s3-chunk-size-mb 10
    --s3-upload-parallel-factor 4
```

### TODO

* [ ] Report ETA and progress.
* [ ] Unit test.
* [ ] Option to write to stdout.
* [ ] Output compression.
