# Proxy requests for the analysis system. Read cached results if possible.
import hug
import pickle
import ast
import os
import json
import requests
import shutil
import urllib
from func_adl.xAOD.backend.ast.ast_hash import calc_ast_hash
import signal
from retry import retry
from queue import Queue
import logging
import uuid
import time
import copy
from threading import Thread

logging.basicConfig(level=logging.INFO)


class BadASTException(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)


class CacheCopyError(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)


class CacheRemoteError(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)


copy_queue = Queue()


@retry(tries=10, delay=0.1)
def create_cache_dir(cache_dir):
    '''Create the cache directory.

    This sometimes has issues with lots and lots of these requests
    come in at the same time. So we have a retry-backoff algorithm.
    This was a bug observed in the wild.
    '''
    if not os.path.isdir(cache_dir):
        if os.path.exists(cache_dir):
            print(f'ERROR: The directory {cache_dir} exists, but is not a directory. Deleting.')
            os.unlink(cache_dir)
        os.makedirs(cache_dir)


@retry(tries=10, delay=0.1)
def fetch_data(ast_data, cache_dir, cache_file, cache_notdone_file):
    'Forward to the remote server to fetch back the data'

    # Get the localtion of the data.
    lm = os.environ["REMOTE_QUERY_URL"]
    logging.info(f'Requesting data from {lm}')
    try:
        raw = requests.post(lm,
                            headers={"content-type": "application/octet-stream"},
                            data=ast_data,
                            timeout=10)
        try:
            r = raw.json()
        except json.decoder.JSONDecodeError:
            raise CacheRemoteError(f'Call to remote func-adl server failed: {raw.content}')

        # Queue up the cacheing
        global copy_queue
        copy_queue.put_nowait(copy.deepcopy((r, cache_dir, cache_file, cache_notdone_file)))
        return r
    finally:
        logging.info(f'Finished data request from {lm}')


@retry(tries=10, delay=0.1)
def rename_file(temp_location, final_location):
    'Rename a file, avoid collisions with others'
    shutil.move(temp_location, final_location)


@retry(tries=10, delay=0.1)
def remote_copy_file(url, cache_dir, final_location):
    'Use xrdp or http download to copy the file locally'
    if os.path.exists(final_location):
        return

    start_time = time.time()

    temp_location = f'{cache_dir}/{str(uuid.uuid4())}'
    try:
        if 'http' in url:
            logging.info(f'Copying file from internet to {final_location} using http')
            get_response = requests.get(url, stream=True)
            with open(temp_location, 'wb') as f:
                for chunk in get_response.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
            os_result = 0
        else:
            logging.info(f'Copying file from internet to {final_location} using root')
            os_result = os.system(f'xrdcp {url} {temp_location}')
        elapsed_time = time.time() - start_time
        logging.info(f'Done doing the copy {final_location} (copy took {elapsed_time:.3f} seconds).')
        if os_result != 0:
            raise CacheCopyError(f'Failed to copy file {url} to {final_location} (os code {os_result}).')
        rename_file(temp_location, final_location)
    finally:
        if os.path.exists(temp_location):
            os.remove(temp_location)


@retry(tries=10, delay=0.1)
def save_cache_file(r, cache_path):
    with open(cache_path, 'w') as f:
        f.write(json.dumps(r))


def process_copy(r, cache_dir, cache_file, cache_notdone_file):
    'Do the copy in a separate thread, and to random files to make sure we do not step of people. and be mindful other threads are doing the same.'

    # Create the cache directory if it doesn't already exist.
    create_cache_dir(cache_dir)

    # Now copy the files locally.
    local_files = []
    for url, t_name in (r['httpfiles'] if 'httpfiles' in r else r['files']):
        p_bits = urllib.parse.urlparse(url)
        f_name = os.path.basename(p_bits.path)
        local_files.append([f_name, t_name])
        final_location = f'{cache_dir}/{f_name}'
        remote_copy_file(url, cache_dir, final_location)
    r['localfiles'] = local_files

    # Now we can cache this. How the caching happens depends if this is a final answer or not.
    cache_path = cache_file if r['done'] else cache_notdone_file
    save_cache_file(r, cache_path)


@retry()
def process_queue():
    global copy_queue
    while True:
        t = copy_queue.get()
        process_copy(*t)
        copy_queue.task_done()


for _ in range(0, 32):
    t = Thread(target=process_queue)
    t.daemon = True
    t.start()


cache_dir = '/cache'


@hug.post('/query')
def query(body):
    r'''
    Given a query (a pickled ast file), return the files or status.
    WARNING: Python AST's are a known security issue and should not be used.

    Arguments:
        body                The Pickle of the python AST representing the request

    Returns:
        Results of the run
    '''
    # If they are sending something too big, then we are just going to bail out of this now.
    if body.stream_len > 1024 * 1000 * 100:
        raise BaseException("Too big an AST to process!")

    # Read the AST in from the incoming data.
    raw_data = body.stream.read(body.stream_len)
    a = pickle.loads(raw_data)
    if a is None or not isinstance(a, ast.AST):
        raise BadASTException(f'Incoming AST is not the proper type: {type(a)}.')

    # Get the hash for it. Do we have a result?
    global cache_dir
    hash = calc_ast_hash(a)
    cache_location = os.path.join(cache_dir, hash)
    cache_result = os.path.join(cache_location, 'result.json')
    cache_notdone_result = os.path.join(cache_location, 'result-notdone.json')
    cache_done_but_not_processed = os.path.join(cache_location, 'result-done.json')

    # Do we have a cache hit?
    if os.path.isfile(cache_result):
        with open(cache_result, 'r') as o:
            result = json.load(o)
    else:
        if os.path.exists(cache_done_but_not_processed):
            with open(cache_done_but_not_processed, 'r') as f:
                result = json.load(f)
        else:
            result = fetch_data(raw_data, cache_location, cache_result, cache_notdone_result)
            if result['done']:
                create_cache_dir(cache_location)
                with open(cache_done_but_not_processed, 'w') as f:
                    json.dump(result, f)

        # The file copies have been queued. We have to return the last
        # thing. We do this b.c. the copies sometimes take some time to get here.
        if os.path.exists(cache_notdone_result):
            with open(cache_notdone_result, 'r') as o:
                data = o.read()
                try:
                    result = json.loads(data)
                except BaseException:
                    print(f"ERROR - failed to load data: '{data}' from file {cache_notdone_result}")
                result['phase'] = 'caching'
        else:
            result['localfiles'] = []
            result['files'] = []
            result['httpfiles'] = []
            if result['done']:
                result['done'] = False
                result['phase'] = 'caching'

    # Add the prefix back in
    if 'localfiles' in result:
        external_cache_location = os.path.join(os.environ['LOCAL_FILE_URL'], hash)
        result['localfiles'] = [[f'{external_cache_location}/{f}', t_name] for f, t_name in result['localfiles']]

    print(f'{hash} - done: {result["done"]} phase: {result["phase"]}')

    return result


# Pay attention to the signal docker and kubectl will send us
# so we can shut down fast.
def do_shutdown(signum, frame):
    exit(1)


signal.signal(signal.SIGTERM, do_shutdown)
