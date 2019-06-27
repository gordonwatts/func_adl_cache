# func_adl_cache

Runs a very simple local cache to a remote `func-adl` server:

- Will cache the data locally (copied over with `xrdcp`)
- Provide back the `root://` address for the remote file, and paths to the local data files.

## Use

You need the following things:

- A location where the system can write the local cache.
- Remote server address

Invocation:

```
docker run -e REMOTE_QUERY_URL=<remote>/query -e LOCAL_FILE_URL=<local-cache-dir> -v <local-cachce-dir>:/cache -d --rm --name func-adl-cache gordonwatts/func-adl-cache:v0.0.1
```
