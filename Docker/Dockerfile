FROM cloudpg/cachingondemand

# Get python3
RUN yum install -y python36 python36-pip

WORKDIR /home/cacher
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Get the code in
COPY func_adl_cache/ .

# Run the server
ENTRYPOINT ["hug"]
CMD ["-f", "./query.py"]
