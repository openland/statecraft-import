import json
import concurrent.futures
import threading
import time
import requests
import math
import os
import pandas as pd

SESSION_THREAD_LOCAL = threading.local()

print("Env: {}".format(os.environ['UPLOAD_SERVER']))

SERVER = "prod"
if 'UPLOAD_SERVER' in os.environ:
    SERVER = os.environ['UPLOAD_SERVER']


def upload_permits(permits):
    initialized = getattr(SESSION_THREAD_LOCAL, 's', None)
    if initialized is None:
        SESSION_THREAD_LOCAL.s = requests.Session()
    start = time.time()
    domain = "sf"
    staging_url = "https://statecraft-api-staging.herokuapp.com/api"
    production_url = "https://statecraft-api.herokuapp.com/api"
    local_url = "http://localhost:9000/api"
    local_docker_url = "http://docker.for.mac.localhost:9000/api"

    if SERVER == "prod":
        url = production_url
    elif SERVER == "staging":
        url = staging_url
        domain = "sfhousing"
    elif SERVER == "docker":
        url = local_docker_url
    else:
        url = local_url

    headers = {
        'x-statecraft-domain': domain,
        'Content-Type': 'application/json'
    }
    container = {
        "query":
        "mutation($args: [PermitInfo]!) { updatePermits(state: \"CA\", county: \"San Francisco\", city: \"San Francisco\", permits: $args) }",
        "variables": {
            "args": permits
        }
    }
    data = json.dumps(container)
    response = SESSION_THREAD_LOCAL.s.post(
        url, data=data, headers=headers, stream=False)
    rdata = json.loads(response.text)
    if rdata['data']['updatePermits'] != 'ok':
        print("Wrong Response!")
        print("Sent:")
        print(data)
        print("Got:")
        print(response.text)
        raise Exception("Wrong response!")
    #    print(r.text)
    end = time.time()
    return end - start


def batch_process_iter(dataset, offset, batch_size, processor):
    start = time.time()
    pending = []
    for _, row in dataset.iloc[offset:offset + batch_size].iterrows():
        pending.append(row)
    if len(pending) > 0:
        processor(BatchBuilder(pending))
    return time.time() - start


def batch_process(dataset, processor, max_workers=1, batch_size=200, limit=-1):
    iterations = math.ceil(len(dataset) / 200)
    if limit >= 0:
        iterations = min(limit, iterations)
    start = time.time()
    if max_workers == 1:
        for i in range(0, iterations):
            res = batch_process_iter(dataset, i * batch_size, batch_size,
                                     processor)
            speed = int((time.time() - start) * 1000) / (i + 1) / 1000.0 / 60.0
            print("Iteration {}/{} in {} remaining {}".format(
                i, iterations, res, (iterations - i) * speed))
    else:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers) as executor:
            pendings = []
            for i in range(0, iterations):
                pendings.append(
                    executor.submit(batch_process_iter, dataset,
                                    i * batch_size, batch_size, processor))
            for i in range(0, iterations):
                res = pendings[i].result()
                speed = int(
                    (time.time() - start) * 1000) / (i + 1) / 1000.0 / 60.0
                print("Iteration {}/{} in {} remaining {}".format(
                    i, iterations, res, (iterations - i) * speed))
    print("Completed in {}".format(time.time() - start))


def validate_string(src: str):
    if isinstance(src, str):
        src = src.strip()
        if src != '':
            return src
        else:
            return None
    else:
        return None


def validate_int(src):
    if src is not None and not math.isnan(src):
        v2 = int(src)
        if v2 > 0 and v2 < 10000:
            return v2
    return None


def validate_date_read(src):
    if src is not None:
        if isinstance(src, str):
            return pd.to_datetime(src)
        else:
            return src
    return None


def validate_date_write(src):
    if src is not None:
        return src.strftime('%Y-%m-%d')
    return None


class BatchBuilder:
    def __init__(self, source):
        self.source = source
        self.index = -1
        self.current_row = {}
        self.data = []

    def read_string(self, src):
        return validate_string(self.source[self.index][src])

    def write_string(self, dst, value):
        self.write_value(dst, validate_string(value))

    def copy_string(self, src, dst):
        self.write_string(dst, self.source[self.index][src])

    def read_int(self, src):
        return validate_int(self.source[self.index][src])

    def write_int(self, dst, value):
        self.write_value(dst, validate_int(value))

    def copy_int(self, src, dst):
        self.write_int(dst, self.read_int(src))

    def read_date(self, src):
        return validate_date_read(self.read_value(src))

    def write_date(self, dst, value):
        self.write_value(dst, validate_date_write(value))

    def copy_date(self, src, dst):
        self.write_date(dst, self.read_date(src))

    def read_value(self, src):
        return self.source[self.index][src]

    def write_value(self, dst, value):
        self.current_row[dst] = value

    def next_record(self):
        if self.index >= 0:
            self.data.append(self.current_row)
            self.current_row = {}
        self.index += 1
        return self.index < len(self.source)
