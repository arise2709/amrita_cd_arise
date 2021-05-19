#!/usr/bin/env python
# coding: utf-8

# In[9]:


import requests
import json
import datetime
import os
import configparser 
import tweepy as tw
config = configparser.RawConfigParser() 
config.read(filenames = 'twitter.properties.txt')
#print(config.sections())

#here we are reading 4 credential keys from the file
accesstoken = config.get('twitter','accesstoken') 
accesstokensecret = config.get('twitter','accesstokensecret') 
apikey = config.get('twitter','apikey') 
apisecretkey = config.get('twitter','apisecretkey')

#Calling OAuthHandler required for authentication with Twitter
auth = tw.OAuthHandler(apikey,apisecretkey) 
auth.set_access_token(accesstoken,accesstokensecret) 

#here we are calling tw.API() .wait_on_rate_limit is rquired because twitter has set a limit
api = tw.API(auth,wait_on_rate_limit=True)

#eg: every user can extract only 15000 tweets at a time
def search_requirement(state,city,requirement):
    state = str(state).lower()
    city =  str(city).lower()
    requirement =str(requirement).lower()
    wanted = '(verified OR available)'
    category = '"%s"' % requirement 
    unwanted ='-"not verified" -"un verified" -"urgent" -"unverified" -"needed" -"required" -"need" -"needs" -"requirement" -"plz" -"where" -"know" -"knows" -"Does anyone" -"Any verified lead" -"Any verified leads" -"filter:retweets"'
    #unwanted ='-"urgent"  -"needed" -"required" -"no" -"requires" -"need" -"needs" -"requirement" -"plz" -"where" -"know" -"knows" -"Does anyone"  -"Any verified lead" -"any leads" -"Any verified leads" -filter:retweets'
    search_word = wanted+" "+city+" "+category+" "+unwanted
    print(search_word)
    #The standard API only allows you to retrieve tweets up to 7 days ago and is limited to scraping 18,000 tweets per a 15 minute window.
    tweets = tw.Cursor(api.search,tweet_mode='extended',q = search_word, lang ='en').items(100)
    tweet_details = [[tweet.id,tweet.user.screen_name,tweet.created_at,tweet.full_text,tweet.user.location]for tweet in tweets]
    import pandas as pd
    tweets_df = pd.DataFrame(tweet_details, columns = ['Tweet ID','username','Tweet posted date', 'Tweet content','location'])
    additional  = ['RT','RTS','retweet']
    tweets_df['processed_text'] = tweets_df['Tweet content'].str.replace('\n',' ')
    #removing duplicates (i.e same tweet retweeted by many people )
    tweets_df.drop_duplicates(subset='processed_text',inplace=True)
    #Tweet preprocessing to obtain state, city, verification and also contact info
    #tweets_df['processed_text'] = tweets_df['Tweet content'].str.lower().str.replace('/',' ').apply(lambda x: [i for i in x.split() if not i in additional])
    tweets_df['processed_text'] = tweets_df['Tweet content'].str.lower()
    requirement=requirement.lower()
    #checking if city is chennai
    for i in tweets_df['processed_text']:
        if (requirement in i) :
            tweets_df['Requirement'] = requirement.capitalize()
        else:
            tweets_df['Requirement'] = 'None'
    import numpy as np
    import pandas as pd
    exclusion = ['not verify','not verified','Not verified','Did not verify']  
    exclude = '|'.join(exclusion)
    inclusion = ['verified','verification done','verified at']
    include = '|'.join(inclusion)
    tweets_df['Verified'] = pd.np.where(tweets_df['Tweet content'].str.lower().str.contains(include),"yes",
                   pd.np.where(tweets_df['Tweet content'].str.lower().str.contains(exclude), "no","no"))
    city=city.lower()
    for i in tweets_df['processed_text']:
        if (city in i) :
            tweets_df['City'] = city.title()
            tweets_df['State'] = state.title()
        else:
            tweets_df['City'] = 'None'
    import re as re
    def find_phone_number(text):
        ph_no = re.findall(r"\b\d{10}\b",text)
        return ", ".join(ph_no)
    tweets_df['Phone number']=tweets_df['Tweet content'].apply(lambda x: find_phone_number(x))
    #removing duplicate as per phone number
    tweets_df.drop_duplicates(subset='Phone number',inplace=True)
    #dropping off unnecessary columns
    tweets_df.drop(['location','processed_text'], axis = 1, inplace = True)
    tweets_df['Tweet content'] = tweets_df['Tweet content'].str.replace('\n',' ').str.replace('ðŸ“',' ').str.replace('âœ…',' ').str.replace('ðŸ“ž',' ')
    base_url = "https://twitter.com/user/status/"
    url =[]
    for i in tweets_df['Tweet ID']:
        url.append(base_url+str(i)) 
    tweets_df['URL'] = url
    import pandas as pd
    tweets_df['Tweet posted date'] = pd.to_datetime(tweets_df['Tweet posted date']).dt.strftime('%d-%m-%Y %H:%M:%S')
    tweets_df
   #tweets_df.to_csv('twitter_data_delhi.csv', header=True, index=False)
    return tweets_df


# In[13]:


chennai_oxygen_concentrator=search_requirement('Tamil Nadu','Chennai','oxygen concentrator' )
#chennai_oxygen_concentrator.to_csv('chennai_oxygen_concentrator.csv', header=True, index=False)


# In[14]:


bangalore_oxygen_concentrator=search_requirement('Karnataka','Bangalore','oxygen concentrator' )
#bangalore_oxygen_concentrator.to_csv('bangalore_oxygen_concentrator.csv', header=True, index=False)


# In[15]:


delhi_oxygen_concentrator=search_requirement('Delhi','Delhi','oxygen concentrator' )
#delhi_oxygen_concentrator.to_csv('delhi_oxygen_concentrator.csv', header=True, index=False)


# In[16]:


chennai_oxygen_refill=search_requirement('Tamil Nadu','Chennai','oxygen refill' )
#chennai_oxygen_refill.to_csv('chennai_oxygen_refill.csv', header=True, index=False)


# In[17]:


bangalore_oxygen_refill=search_requirement('Karnataka','Bangalore','oxygen refill' )
#bangalore_oxygen_refill.to_csv('bangalore_oxygen_refill.csv', header=True, index=False)


# In[18]:


delhi_oxygen_refill=search_requirement('Delhi','Delhi','oxygen refill' )
#delhi_oxygen_refill.to_csv('delhi_oxygen_refill.csv', header=True, index=False)


# In[19]:


chennai_oxygen_bed=search_requirement('Tamil Nadu','Chennai','oxygen bed' )
#chennai_oxygen_bed.to_csv('chennai_oxygen_bed.csv', header=True, index=False)


# In[20]:


bangalore_oxygen_bed=search_requirement('Karnataka','Bangalore','oxygen bed' )
#bangalore_oxygen_bed.to_csv('bangalore_oxygen_bed.csv', header=True, index=False)


# In[21]:


delhi_oxygen_bed=search_requirement('Delhi','Delhi','Oxygen bed' )
#delhi_oxygen_bed.to_csv('delhi_oxygen_bed.csv', header=True, index=False)


# In[22]:


chennai_icu_bed=search_requirement('Tamil Nadu','Chennai','ICU bed' )


# In[23]:


bangalore_icu_bed=search_requirement('Karnataka','Bangalore','ICU bed' )


# In[29]:


delhi_icu_bed=search_requirement('Delhi','Delhi','ICU bed' )


# In[25]:


chennai_plasma=search_requirement('Tamil Nadu','Chennai','Plasma')


# In[26]:


bangalore_plasma=search_requirement('Karnataka','Bangalore','Plasma')


# In[34]:


delhi_plasma=search_requirement('Delhi','Delhi','Plasma' )


# In[42]:


import csv
import pandas as pd
frames =[chennai_oxygen_concentrator,bangalore_oxygen_concentrator,delhi_oxygen_concentrator,chennai_oxygen_refill,bangalore_oxygen_refill,delhi_oxygen_refill,chennai_oxygen_bed,bangalore_oxygen_bed,delhi_oxygen_bed,chennai_icu_bed,bangalore_icu_bed,delhi_icu_bed,chennai_plasma,bangalore_plasma,delhi_plasma]
result = pd.concat(frames)
result.to_excel('twitter op1.xlsx', header=True, index=False)

