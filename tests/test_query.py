# Test the query caching system. Lots of mocks!
import sys
sys.path.append('.')
from func_adl_cache.query import query, CacheCopyError, CacheRemoteError
from adl_func_client.event_dataset import EventDataset
import pytest
import pickle
import json
import io
import os
import shutil
from unittest.mock import Mock, PropertyMock

@pytest.fixture
def good_query_ast_pickle_data():
    'A good query ast to be used for testing below'
    f_ds = EventDataset(r'localds://mc16_13TeV.311309.MadGraphPythia8EvtGen_A14NNPDF31LO_HSS_LLP_mH125_mS5_ltlow.deriv.DAOD_EXOT15.e7270_e5984_s3234_r9364_r9315_p3795')
    a = f_ds \
        .SelectMany('lambda e: e.Jets("AntiKt4EMTopoJets")') \
        .Select('lambda j: j.pt()/1000.0') \
        .AsROOTTTree('output.root', 'dudetree', 'JetPt') \
        .value(executor=lambda a: a)
    return pickle.dumps(a)

@pytest.fixture
def already_done_ds(monkeypatch):
    push_mock = Mock()
    status_mock = Mock()
    push_mock.return_value = status_mock
    monkeypatch.setattr('requests.post', push_mock)
    status_mock.json.return_value={'files': [['http://remote:8000/blah/file.root', 'dudetree3']], 'phase': 'done', 'done': True, 'jobs': 1}
    return push_mock

@pytest.fixture
def not_done_ds(monkeypatch):
    push_mock = Mock()
    status_mock = Mock()
    push_mock.return_value = status_mock
    monkeypatch.setattr('requests.post', push_mock)
    status_mock.json.return_value={'files': [], 'phase': 'running', 'done': False, 'jobs': 1}

@pytest.fixture
def failed_ds_request(monkeypatch):
    push_mock = Mock()
    status_mock = Mock()
    push_mock.return_value = status_mock
    monkeypatch.setattr('requests.post', push_mock)
    try:
        json.loads(b'the server had an error')
    except BaseException as e:
        status_mock.json = Mock(side_effect=e)
    status_mock.content.return_value = b'The server had an error'

@pytest.fixture
def setup_cache():
    c_path = os.path.join(os.getcwd(), 'cache')
    if os.path.exists(c_path):
        shutil.rmtree(c_path)
    import func_adl_cache.query
    func_adl_cache.query.cache_dir = c_path
    yield c_path
    if os.path.exists(c_path):
        shutil.rmtree(c_path)

@pytest.fixture
def no_prefix_env(setup_cache):
    os.environ['CACHE_DIR'] = setup_cache
    os.environ['REMOTE_QUERY_URL'] = 'http://remote:8000'
    os.environ['LOCAL_FILE_URL'] = 'file:///usr/local'
    yield 'ok'
    del os.environ['CACHE_DIR']
    del os.environ['REMOTE_QUERY_URL']
    del os.environ['LOCAL_FILE_URL']

class Holder:
    def __init__ (self, b):
        self.stream = io.BytesIO(b)
        self.stream_len = len(b)

@pytest.fixture
def good_query_ast_body(good_query_ast_pickle_data):
    return Holder(good_query_ast_pickle_data)

@pytest.fixture
def good_query_ast_body2(good_query_ast_pickle_data):
    return Holder(good_query_ast_pickle_data)

@pytest.fixture
def good_copy_command(monkeypatch):
    os_system_call = Mock()
    os_system_call.return_value = 0
    monkeypatch.setattr('os.system', os_system_call)

@pytest.fixture
def bad_2nd_copy_command(monkeypatch):
    os_system_call = Mock()
    os_system_call.return_value = [0, 1]
    monkeypatch.setattr('os.system', os_system_call)

@pytest.fixture
def rmtree_call(monkeypatch):
    rmtree = Mock()
    rmtree.return_value = 0
    monkeypatch.setattr('shutil.rmtree', rmtree)
    return rmtree

def test_good_call_no_prefix(good_query_ast_body, no_prefix_env, already_done_ds, good_copy_command):
    r = query(good_query_ast_body)
    assert len(r['files']) == 1
    assert r['files'][0][0].replace('\\','/') == 'http://remote:8000/blah/file.root'
    assert len(r['localfiles']) == 1
    assert r['localfiles'][0][0].replace('\\','/') == 'file:///usr/local/ba2275c1edda01df1775f72108067c30/file.root'
    assert r['done'] == True

def test_copy_fails_party_way_through(good_query_ast_body, no_prefix_env, already_done_ds, bad_2nd_copy_command, rmtree_call):
    'Fail with the copy part way, make sure the whole cache is invalid'
    try:
        r = query(good_query_ast_body)
        assert False
    except CacheCopyError:
        pass
    
    # Make sure the remove was done on the cache.
    rmtree_call.assert_called()

def test_bad_remote_call(good_query_ast_body, no_prefix_env, failed_ds_request):
    'Fail with the copy part way, make sure the whole cache is invalid'
    try:
        r = query(good_query_ast_body)
        assert False
    except CacheRemoteError:
        pass

def test_cache_lookup(good_query_ast_body, good_query_ast_body2, no_prefix_env, already_done_ds, good_copy_command):
    _ = query(good_query_ast_body)

    r = query(good_query_ast_body2)
    assert len(r['files']) == 1
    assert r['files'][0][0].replace('\\','/') == 'http://remote:8000/blah/file.root'
    assert len(r['localfiles']) == 1
    assert r['localfiles'][0][0].replace('\\','/') == 'file:///usr/local/ba2275c1edda01df1775f72108067c30/file.root'

    already_done_ds.assert_called_once()

def test_ds_not_done(good_query_ast_body, no_prefix_env, not_done_ds):
    r = query(good_query_ast_body)
    assert r['done'] == False
