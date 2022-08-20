# twitter-sentiment-analysis-app
This app allows you to monitor the real-time sentiment of specific topics on twitter. This project serves as a means for me to apply the theory of large scale data processing by building a real-time data processing pipeline using open source tools. 

Live dashboard: http://35.247.129.63:80

![alt text](https://github.com/YongKuok/twitter-sentiment-analysis-app/blob/main/streamlit_dashboard_preview.gif) <br />

## Architecture
![alt text](https://github.com/YongKuok/twitter-sentiment-analysis-app/blob/main/workflow.png) <br />
Tweet data is captured in real-time from the Twitter API using Tweepy python libary and streamed to Kafka. Apache Spark consumes the data from the Kafka stream, processes it and assigns a sentiment score using pretrained VADER sentiment analyzer from Python's NLTK library. The labelled data is then stored in postgreSQL and analysed through a Streamlit dashboard. 

## Setup instructions
1. Clone the project repository.
2. Install requirements using package manager [pip](https://pip.pypa.io/en/stable/).
```bash
pip install -r requirements.txt
```
3. Download [Apache Kafka](https://www.apache.org/dyn/closer.cgi?path=/kafka/3.2.0/kafka_2.13-3.2.0.tgz), [PostgreSQL](https://www.postgresql.org/download/linux/ubuntu/) and [PostgreSQL JDBC driver](https://jdbc.postgresql.org/download.html) for Apache Spark (copy JDBC driver to $SPARK_HOME/jars directory).
4. Download VADER lexicon for sentiment analysis.
```bash
python3 -m nltk.downloader vader_lexicon
```
5. Create a Twitter development account [here](https://developer.twitter.com/en/apply-for-access) and update the credentials.conf file with the respective Twitter API key and postgreSQL credentials.

## Usage
1. Start Zookeeper server. 
```bash
cd $KAFKA_HOME
bin/zookeeper-server-start.sh config/zookeeper.properties
```
2. Start Apache Kafka server.
```bash
bin/kafka-server-start.sh config/server.properties
```
3. Run Kafka producer script from the project repository.
```bash
cd /path_to_folder/twitter-sentiment-analysis-app
python3 /src/kafka_tweepy_producer.py
```
4. Run Apache Spark job.
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 /src/spark_processor.py
```
5. Run Streamlit dashboard.
```bash
streamlit run /src/my_app.py
```
Access the Streamlit dashboard using http://localhost:8501
