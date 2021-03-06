#!/usr/bin/env python3

import ctypes
import io
from queue import Empty
from datetime import datetime
from os import cpu_count, path
import sys
import json
import logging
from multiprocessing import Process, Queue, Value
from typing import Callable

import boto3


KB = 1024
MB = KB * KB
MEMORY_LIMIT = 500 * MB


def dump(
    *,
    table_name,
    total_segments,
    s3_bucket,
    s3_prefix,
    s3_chunk_size_mb,
    s3_upload_parallel_factor,
):
    """Dump a DynamoDB table to a S3 bucket in parallel."""
    page_queue = Queue()
    chunk_queue = Queue(maxsize=MEMORY_LIMIT // s3_chunk_size_mb // MB)

    readers_countdown = Value(ctypes.c_int, total_segments)
    chunkers_countdown = Value(ctypes.c_int, 1)

    read_processes = []
    for segment in range(total_segments):
        read_processes.append(
            Process(
                target=WithCountdown(read_table_segment_worker, readers_countdown),
                kwargs=dict(
                    table_name=table_name,
                    segment=segment,
                    total_segments=total_segments,
                    page_queue=page_queue,
                ),
            )
        )

    to_chunks_process = Process(
        target=WithCountdown(to_chunks_worker, chunkers_countdown),
        kwargs=dict(
            queue_in=page_queue,
            queue_out=chunk_queue,
            s3_chunk_size_mb=s3_chunk_size_mb,
            number_of_queue_writers=readers_countdown,
        ),
    )

    write_processes = []
    for _ in range(s3_upload_parallel_factor):
        write_processes.append(
            Process(
                target=upload_chunks_worker,
                kwargs=dict(
                    queue=chunk_queue,
                    s3_bucket=s3_bucket,
                    s3_prefix=s3_prefix,
                    number_of_queue_writers=chunkers_countdown,
                ),
            )
        )

    processes = read_processes + [to_chunks_process] + write_processes
    for p in processes:
        p.start()
    for p in processes:
        p.join()


class WithCountdown:
    """
    Decorator to communicate upstream the number of alive workers.

    If implemented as a function with functools.wraps it fails pickling.
    """

    def __init__(self, func: Callable, countdown: Value):
        self.func = func
        self.countdown = countdown

    def __call__(self, *args, **kwargs):
        """Decrement the countdown value after each execution of the function."""
        try:
            return self.func(*args, **kwargs)
        finally:
            with self.countdown.get_lock():
                self.countdown.value -= 1
            logger.debug(
                f"{self.func.__name__} exited. {self.countdown.value} running."
            )


def read_table_segment_worker(
    *,
    table_name: str,
    segment: int,
    total_segments: int,
    page_queue: "Queue[bytes]",
):
    """
    Scan recursively a DynamoDB table's segment,
    serialize pages to bytes and put them on the queue.

    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
    """

    def serialize(page):
        return ("\n".join([json.dumps(item) for item in page["Items"]]) + "\n").encode()

    for page in (
        boto3.client("dynamodb")
        .get_paginator("scan")
        .paginate(
            TableName=table_name,
            Segment=segment,
            TotalSegments=total_segments,
        )
    ):
        page_queue.put(serialize(page))
        logger.info(
            f"Page downloaded."
            f" Segment: {segment+1}/{total_segments}."
            f" Number of items: {len(page['Items'])}."
        )


class Chunk:
    """Wrap BytesIO to be passed as a file-like object for reading."""

    def __init__(self, number=1):
        self.number = number
        self.buffer = io.BytesIO()

    def write(self, bytes):
        self.buffer.write(bytes)

    def size(self):
        return self.buffer.tell()

    def fileobj(self):
        self.buffer.seek(0)
        return self.buffer

    def next(self):
        return Chunk(self.number + 1)


def to_chunks_worker(
    *,
    queue_in: "Queue[bytes]",
    queue_out: "Queue[Chunk]",
    s3_chunk_size_mb: int = 10,
    number_of_queue_writers: Value,
):
    """Repack bytes from the incoming queue to the outgoing queue by the chunk size."""

    chunk = Chunk()
    while number_of_queue_writers.value > 0 or not queue_in.empty():
        try:
            data = queue_in.get(timeout=1)
        except Empty:
            continue
        chunk.write(data)
        if chunk.size() > s3_chunk_size_mb * MB:
            queue_out.put(chunk)
            logger.debug(f"Chunk ready #{chunk.number}.")
            chunk = chunk.next()

    if chunk.size() > 0:
        queue_out.put(chunk)
        logger.debug(f"Last chunk #{chunk.number}.")


def upload_chunks_worker(
    *,
    queue: "Queue[Chunk]",
    s3_bucket: str,
    s3_prefix: str,
    number_of_queue_writers: Value,
):
    """Upload chunks from the queue."""
    client = boto3.client("s3")

    while number_of_queue_writers.value > 0 or not queue.empty():
        try:
            chunk = queue.get(timeout=1)
        except Empty:
            continue
        file_name = f"part_{chunk.number:03}.json"
        logger.info(f"Uploading {(chunk.size() / MB):.1f}Mb chunk {file_name}... ")
        client.upload_fileobj(chunk.fileobj(), s3_bucket, s3_prefix + file_name)
        logger.info(f"Uploaded chunk {file_name}.")
    logger.debug("upload_chunks_worker exited.")


def option_required(name) -> str:
    """A poor person's argparse."""
    return sys.argv[sys.argv.index(f"--{name}") + 1]


def option(name, default=None) -> str:
    try:
        return option_required(name)
    except ValueError:
        return str(default)


logger = logging.getLogger(path.basename(__file__))
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s [%(levelname)s] - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
log_level = option("log-level", "info")
logger.setLevel(log_level.upper())

if __name__ == "__main__":

    # Source options
    table_name = option_required("table-name")
    total_segments = int(option("total-segments", default=cpu_count()))

    # Target options
    run_time = datetime.now().isoformat() + "Z"
    default_s3_prefix = f"raw/{table_name}/dynamodb/full-load/run_time={run_time}/"
    s3_prefix = option("s3-prefix", default=default_s3_prefix)
    s3_bucket = option_required("s3-bucket")
    s3_chunk_size_mb = int(option("s3-chunk-size-mb", default=50))
    s3_upload_parallel_factor = int(option("s3-upload-parallel-factor", default=4))

    logger.debug(
        "Initialized with options: %s.",
        dict(
            table_name=table_name,
            total_segments=total_segments,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            s3_chunk_size_mb=s3_chunk_size_mb,
            s3_upload_parallel_factor=s3_upload_parallel_factor,
        ),
    )

    dump(
        table_name=table_name,
        total_segments=total_segments,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        s3_chunk_size_mb=s3_chunk_size_mb,
        s3_upload_parallel_factor=s3_upload_parallel_factor,
    )

    logger.debug("Completed.")
