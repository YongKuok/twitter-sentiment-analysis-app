import streamlit as st
import pandas as pd
import configparser
from datetime import datetime
from datetime import timedelta
import psycopg2
import kafka_producer as tw

parser = configparser.ConfigParser(interpolation=None)
parser.read("credentials.conf")
# psql
psql_user = parser.get("postgres_config", "user")
psql_password = parser.get("postgres_config", "password")
psql_host = parser.get("postgres_config", "host")
psql_port = parser.get("postgres_config", "port")
psql_database = parser.get("postgres_config", "database")
# tweepy client
tw_bearer_token = parser.get("tweepy_config", "bearer_token")

def main():
    twitter_streamer = tw.TwitterStreamer(tw_bearer_token)

    ### App layout ###
    header = st.container()
    dataset = st.container()

    with header:
        st.title("Real-time twitter sentiment analysis")
        st.text("App for monitoring real-time sentiments of tweets based on specific keywords.")

    with dataset:
        st.title("Tweet statistics")

    ### App sidebar ###
    # Data range
    start_timing_options = {
        'Past 12 hours': 12,
        'Past 24 hours': 24,
        'Past 72 hours': 72
    }
    start_timing_box = st.sidebar.selectbox(label='Data range:', options=list(start_timing_options.keys()), index=1)
    start_time = start_timing_options[start_timing_box]

    # Plot frequency
    plot_freq_options = {
        '5 Min' : '5T',
        '15 Min' : '15T',
        'Hourly': 'H',
        'Four Hourly': '4H',
        'Daily': 'D'
    }
    plot_freq_box = st.sidebar.selectbox(label='Plot Frequency:', options=list(plot_freq_options.keys()), index=1)
    plot_freq = plot_freq_options[plot_freq_box]

    # Keywords tracking
    input_keyword = st.sidebar.text_input(label='Input keyword to track:')

    add_keyword_res = add_keyword(twitter_streamer, input_keyword)
    active_keywords = get_keywords(twitter_streamer)
    retained_keywords = st.sidebar.multiselect(label='Keywords tracked:', options=active_keywords['keyword'], default=active_keywords['keyword'])
    remove_keywords_id = [y for x, y in zip(active_keywords['keyword'],active_keywords['id']) if x not in retained_keywords]
    remove_keyword_res = remove_keywords(twitter_streamer, remove_keywords_id)

    ### Extracting data from PostgreSQL ###
    plot_data = load_data(start_time, plot_freq, twitter_streamer)

    ### Plotting charts ###
    dataset.subheader('Tweet Sentiment')
    params = [kw + '_score' for kw in active_keywords['keyword']]
    plot_data_sentiment = plot_data[params]
    dataset.line_chart(plot_data_sentiment)
    polarity_eq = r'''Tweet \; sentiment = {p-n \over p+n} \newline where\; p=No.\; of\; positive\; tweets \newline \qquad \quad n=No.\; of\; negative\; tweets'''
    dataset.latex(polarity_eq)

    dataset.subheader('Tweet Volume')
    plot_data_volume = plot_data[list(active_keywords['keyword'])]
    dataset.line_chart(plot_data_volume)

def init_connection():
    return psycopg2.connect(
        user=psql_user,
        password=psql_password,
        host=psql_host,
        port=psql_port,
        database=psql_database
        )

def run_query(connection, query, param=()):
    with connection.cursor() as cur:
        cur.execute(query,param)
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns = colnames)
        return df

def load_data(start_time, plot_freq, twitter_client):
    ### Extracting data from PostgreSQL ###
    start_datetime = datetime.now() - timedelta(hours = start_time)
    conn = init_connection() 
    data = run_query(conn, "select creation_timestamp,keyword,polarity_score from tweets WHERE creation_timestamp > %s", param=(start_datetime,))
    conn.close()
    active_keywords = get_keywords(twitter_client)
    plot_data = compile_plot_data(data, list(active_keywords['keyword']), plot_freq)
    return plot_data

def compile_plot_data(df, keywords, freq):
    # get pos/neg/neu count for each keyword
    for kw in keywords:
        df[kw] = 0
        df[kw+'_pos'] = 0
        df[kw+'_neg'] = 0
        df[kw+'_neu'] = 0
        df.loc[df['keyword'] == kw, kw] = 1 
        df.loc[(df['keyword'] == kw) & (df['polarity_score'] > 0), kw+'_pos'] = 1
        df.loc[(df['keyword'] == kw) & (df['polarity_score'] < 0), kw+'_neg'] = 1
        df.loc[(df['keyword'] == kw) & (df['polarity_score'] == 0), kw+'_neg'] = 1

    cols = ['creation_timestamp']
    cols.extend(keywords)
    for kw in keywords:
        cols.append(kw+'_pos')
        cols.append(kw+'_neg')
        cols.append(kw+'_neu')
    plot_data = df[cols]

    if len(plot_data) == 0:
        return plot_data
    else:
        plot_data = plot_data.resample(freq, on='creation_timestamp').sum()
        # Calculating polarity of each keyword
        for kw in keywords:
            plot_data[kw+'_score'] = 0
            plot_data.loc[(plot_data[kw+'_pos']>0) | (plot_data[kw+'_neg']>0), kw+'_score'] = (plot_data[kw+'_pos'] - plot_data[kw+'_neg'])/(plot_data[kw+'_pos']+plot_data[kw+'_neg'])
        return plot_data

def get_keywords(streaming_client):
    keyword_df = pd.DataFrame(streaming_client.get_rules().data)
    keyword_df = keyword_df.rename(columns={'tag':'keyword'})
    return keyword_df

def add_keyword(streaming_client, input_keyword):
    if len(input_keyword) > 0:
        rule = streaming_client.create_rule(input_keyword)
        streaming_client.add_rules(rule)
        return "Keyword: {} added.".format(input_keyword)

def remove_keywords(streaming_client, list_ids):
    for id in list_ids:
        streaming_client.delete_rules(id)
    return "Keyword ids: {} removed.".format(list_ids)
 
    
if __name__ == "__main__":
    main()
