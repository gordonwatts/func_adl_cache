FROM cloudpg/cachingondemand

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
ENTRYPOINT ["hug"]
CMD ["-f", "./query.py"]
