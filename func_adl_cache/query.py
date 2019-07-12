# Proxy requests for the analysis system. Read cached results if possible.
import hug
import pickle
import ast
import os
import json
import uuid
import requests
import shutil
import urllib
from adl_func_backend.ast.ast_hash import calc_ast_hash
import signal
from retry import retry

class BadASTException(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)

class CacheCopyError(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)

class CacheRemoteError(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)

@retry(tries=10, delay=0.1)
def create_cache_dir(cache_dir):
    '''Create the cache directory.

    This sometimes has issues with lots and lots of these requests
    come in at the same time. So we have a retry-backoff algorithm.
    This was a bug observed in the wild.
    '''
    if not os.path.isdir(cache_dir):
        if os.path.exists(cache_dir):
            print (f'ERROR: The directory {cache_dir} exists, but is not a directory. Deleting.')
            os.unlink(cache_dir)
        os.makedirs(cache_dir)

def fetch_data(a, cache_dir):
    'Forward to the remote server to fetch back the data'
    ast_data = pickle.dumps(a)

    # Get the localtion of the data.
    lm = os.environ["REMOTE_QUERY_URL"]
    print (f'Requesting data from {lm}')
    raw = requests.post(lm,
        headers={"content-type": "application/octet-stream"},
        data=ast_data)
    try:
        r = raw.json()
    except json.decoder.JSONDecodeError:
        raise CacheRemoteError(f'Call to remote func-adl server failed: {raw.content}')

    # Now copy the files locally.
    local_files = []
    create_cache_dir(cache_dir)

    for url,t_name in r['files']:
        p_bits = urllib.parse.urlparse(url)
        f_name = os.path.basename(p_bits.path)
        local_files.append([f_name, t_name])
        final_location = f'{cache_dir}/{f_name}'
        os_result = os.system(f'xrdcp {url} {final_location}')
        if os_result != 0:
            raise CacheCopyError(f'Failed to copy file {url} to {final_location}.')
    r['localfiles'] = local_files
    return r

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
    if body.stream_len > 1024*1000*100:
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

    # Do we have a cache hit?
    if os.path.isfile(cache_result):
        with open(cache_result, 'r') as o:
            result = json.load(o)
    else:
        local_cache_item_dir = os.path.join(cache_dir,hash)
        try:
            result = fetch_data(a, local_cache_item_dir)
        except:
            if os.path.exists(local_cache_item_dir):
                shutil.rmtree(local_cache_item_dir)
            raise

        # Only cache if the job is done.
        if result['done'] == True:
            with open(cache_result, 'w') as o:
                json.dump(result, o)

    # Add the prefix back in
    external_cache_location = os.path.join(os.environ['LOCAL_FILE_URL'], hash)
    result['localfiles'] = [[f'{external_cache_location}/{f}', t_name] for f, t_name in result['localfiles']]

    return result

# Pay attention to the signal docker and kubectl will send us
# so we can shut down fast.
def do_shutdown(signum, frame):
    exit(1)

signal.signal(signal.SIGTERM, do_shutdown)
