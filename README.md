# pyspark2025
A pyspark refresher 


## Prerequisites

- **Python 3.6**: Python 3.6 and higher.
- **Apache Spark**: Docker container apache/spark-py

##

>Setup the python environment:

```bash
# cd project root directory
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```


>Run a standalone spark instance

```bash
docker run -d apache/spark-py 
# For interactive mode 
# docker run -d apache/spark-py  /opt/spark/bin/pyspark