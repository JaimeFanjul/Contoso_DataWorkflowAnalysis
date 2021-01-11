import pandas as pd
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import nltk
from textblob import TextBlob
import re
import csv
import time

df = pd.read_csv('data_contoso.csv.gz', compression='gzip')

# remove the color of the productname
df['productname'] = df['productname'].map(lambda x: ' '.join(x.split(' ')[:-1]))

# grouping to get criteria
group_columns = df.columns
unwanted_fields = ['salesquantity', 'returnquantity', 'unitcost', 'unitprice']
group_columns = [i for i in group_columns if i not in unwanted_fields]
df = df.groupby(group_columns).agg({'salesquantity':['sum'],
                               'returnquantity':['sum'],
                               'unitcost':['mean'],
                               'unitprice':['mean']})
# remove the multindexing
df.columns = df.columns.droplevel(level=1)
df = df.reset_index()

def set_store_type(store_name):
    if 'Online' in store_name:
        return 'Online'
    elif 'Catalog' in store_name:
        return 'Catalog'
    else:
        return 'Store'

df['storetype'] = df['storename'].map(lambda x: set_store_type(x))

# get the store locations
def extract_city(store_name):
    n_words = len(store_name.split(' '))
    if n_words == 3:
        return store_name.split(' ')[1]
    elif n_words == 4:
        if 'No.' in store_name:
            return  store_name.split(' ')[1]
        else:
            return  ' '.join(store_name.split(' ')[1:3])
    elif n_words == 5:
        if 'No.' in store_name:
            return  ' '.join(store_name.split(' ')[1:3])
        else:
            return  ' '.join(store_name.split(' ')[1:4])

df['storename'] = df['storename'].map(lambda x: x.replace('  ', ' '))
df['city'] = df['storename'].map(lambda x: extract_city(x))

# get the correct locations in EEUU
df_zips = pd.read_csv('uszips.csv.gz', compression='gzip')

# select the binomial city/sate with the hightest population
df_zips = df_zips[['state_name', 'city', 'population']].groupby(['state_name', 'city']).sum().reset_index()
df_zips = df_zips.sort_values('population', ascending=False)
df_zips = df_zips.groupby('city').head(1)

# put it all together and fill the NAN values
df = df.merge(df_zips[['city', 'state_name']], how='left', on='city')
df.fillna('N/A', inplace=True)

df.to_csv('data_contoso_processed.csv.gz', index=False,  compression='gzip')

# reviews processing using NLP
df_reviews = pd.read_csv('data_contoso_yelp_reviews.csv.gz', compression='gzip')

df_reviews['text_nlp'] = df_reviews['text'].map(lambda x: " ".join(x.lower() for x in x.split()))
## remove punctuation
df_reviews['text_nlp'] = df_reviews['text_nlp'].str.replace('[^\w\s]','')

# empty words
stop = stopwords.words('english')
df_reviews['text_nlp'] = df_reviews['text_nlp'].map(lambda x: " ".join(x for x in x.split() if x not in stop))

# text radicalization
# use st.stem(word) to radicalize a word
st = PorterStemmer()
df_reviews['text_nlp'] = df_reviews['text_nlp'].map(lambda x: " ".join([st.stem(word) for word in x.split()]))

# sentiment processing
def analize_sentiment(text):
    '''
    Utility function to classify the polarity of an opinion
    using textblob.
    '''
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return ('Positive', analysis.sentiment.polarity)
    elif analysis.sentiment.polarity == 0:
        return ('Neutral', analysis.sentiment.polarity)
    else:
        return ('Negative', analysis.sentiment.polarity)

df_reviews['polarity'], df_reviews['polarityscore'] =  zip(*df_reviews['text'].map(lambda x: analize_sentiment(x)))
df_reviews.to_csv('data_yelp_processed.csv.gz', index=False, sep='\t', compression='gzip')
