ARG SPARK_VERSION=v3.3.1
FROM apache/spark-py:${SPARK_VERSION}
# using root user
USER root:root

# create the directory that will store the spark jobs
RUN mkdir sparkingest

RUN pip install pyspark==3.3.1

# copy spark jobs local to image
COPY __init__.py sparkingest/__init__.py
COPY __main__.py sparkingest/__main__.py
COPY sparkInit.py sparkingest/sparkInit.py

# set python3
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYTHONPATH=.

CMD ["python3", "sparkingest"]