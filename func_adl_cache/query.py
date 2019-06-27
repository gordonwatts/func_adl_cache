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

class BadASTException(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)

class CacheCopyError(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)

class CacheRemoteError(BaseException):
    def __init__(self, message):
        BaseException.__init__(self, message)

def fetch_data(a, cache_dir, cache_dir_external):
    'Forward to the remote server to fetch back the data'
    ast_data = pickle.dumps(a)

    # Get the localtion of the data.
    lm = os.environ["REMOTE_QUERY_URL"]
    raw = requests.post(lm,
        headers={"content-type": "application/octet-stream"},
        data=ast_data)
    try:
        r = raw.json()
    except json.decoder.JSONDecodeError:
        raise CacheRemoteError(f'Call to remote func-adl server failed: {raw.content}')

    # Now copy the files locally.
    local_files = []
    os.mkdir(cache_dir)

    for url,t_name in r['files']:
        p_bits = urllib.parse.urlparse(url)
        f_name = os.path.basename(p_bits.path)
        local_files.append([f'{cache_dir_external}/{f_name}', t_name])
        final_location = f'{cache_dir}/{f_name}'
        os_result = os.system(f'xrdcp {url} {final_location}')
        if os_result != 0:
            raise CacheCopyError(f'Failed to copy file {url} to {final_location}.')
    r['localfiles'] = local_files
    return r

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
    hash = calc_ast_hash(a)
    cache_dir = os.environ['CACHE_DIR']
    cache_location = os.path.join(cache_dir, hash)
    cache_result = os.path.join(cache_location, 'result.json')

    # Do we have a cache hit?
    if os.path.isfile(cache_result):
        result = json.load(cache_result)
    else:
        try:
            result = fetch_data(a, cache_dir, os.environ['LOCAL_FILE_URL'] + '/' + hash)
        except:
            if os.path.exists(cache_dir):
                shutil.rmtree(cache_dir)
            raise

        if not os.path.isdir(os.path.dirname(cache_result)):
            os.makedirs(os.path.dirname(cache_result))
        with open(cache_result, 'w') as o:
            json.dump(result, o)

    return result