FROM cloudpg/cachingondemand:k8s-v1

# Get python3
RUN yum install -y python36 python36-pip

WORKDIR /home/cacher
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Get the code in
COPY func_adl_cache/ .

# Turn this on so that stdout isn't buffered - otherwise logs in kubectl don't
# show up until much later!
ENV PYTHONUNBUFFERED=1

# Run the server
ENTRYPOINT ["gunicorn"]
CMD ["--bind", "0.0.0.0:8000", "-w", "1", "--log-file", "-", "--log-level", "debug", "query:__hug_wsgi__"]
