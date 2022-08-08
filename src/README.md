## credentials.conf file in src contains

    [tweepy_config]
    bearer_token =<tweepy API bearer token>

    [postgres_config]
    user= <postgres username>
    password= <postgres password>
    url= jdbc:postgresql://localhost:5432/<database name>
    host= localhost
    port= 5432
    database= <database name>
    table= <data table name>

    [kafka_config]
    kafka_topic_name= <kafka topic name>
    kafka_bootstrap_servers= localhost:9092