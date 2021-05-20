#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


chennai_oxygen_concentrator=search_requirement('Tamil Nadu','Chennai','oxygen concentrator' )
bangalore_oxygen_concentrator=search_requirement('Karnataka','Bangalore','oxygen concentrator' )
delhi_oxygen_concentrator=search_requirement('Delhi','Delhi','oxygen concentrator' )
#chennai_oxygen_concentrator.to_csv('chennai_oxygen_concentrator.csv', header=True, index=False)


# In[3]:


chennai_oxygen_cylinder = search_requirement('TamilNadu','Chennai','oxygen cylinder')
bangalore_oxygen_cylinder=search_requirement('Karnataka','Bangalore','oxygen cylinder')
delhi_oxygen_cylinder=search_requirement('Delhi','Delhi','oxygen cylinder' )


# In[4]:


#chennai_oxygen_refill=search_requirement('Tamil Nadu','Chennai','oxygen refill' )
#bangalore_oxygen_refill=search_requirement('Karnataka','Bangalore','oxygen refill' )
#delhi_oxygen_refill=search_requirement('Delhi','Delhi','oxygen refill' )


# In[5]:


chennai_oxygen_bed=search_requirement('Tamil Nadu','Chennai','oxygen bed' )
bangalore_oxygen_bed=search_requirement('Karnataka','Bangalore','oxygen bed' )
delhi_oxygen_bed=search_requirement('Delhi','Delhi','Oxygen bed' )
#chennai_oxygen_bed.to_csv('chennai_oxygen_bed.csv', header=True, index=False)


# In[6]:


chennai_icu_bed=search_requirement('Tamil Nadu','Chennai','ICU bed' )
bangalore_icu_bed=search_requirement('Karnataka','Bangalore','ICU bed' )
delhi_icu_bed=search_requirement('Delhi','Delhi','ICU bed' )


# In[7]:


chennai_plasma=search_requirement('Tamil Nadu','Chennai','Plasma')
bangalore_plasma=search_requirement('Karnataka','Bangalore','Plasma')
delhi_plasma=search_requirement('Delhi','Delhi','Plasma' )


# In[8]:


chennai_food = search_requirement('TamilNadu','Chennai','Food')
bangalore_food=search_requirement('Karnataka','Bangalore','Food')
delhi_food=search_requirement('Delhi','Delhi','Food' )


# In[9]:


chennai_ventilator = search_requirement('TamilNadu','Chennai','Ventilator')
bangalore_ventilator=search_requirement('Karnataka','Bangalore','Ventilator')
delhi_ventilator=search_requirement('Delhi','Delhi','Ventilator' )


# In[10]:


chennai_normal_bed = search_requirement('TamilNadu','Chennai','normal beds')
bangalore_normal_bed=search_requirement('Karnataka','Bangalore','normal beds')
delhi_normal_bed=search_requirement('Delhi','Delhi','normal beds' )


# In[11]:


chennai_amphotericin = search_requirement('TamilNadu','Chennai','Amphotericin')
bangalore_amphotericin=search_requirement('Karnataka','Bangalore','Amphotericin')
delhi_amphotericin=search_requirement('Delhi','Delhi','Amphotericin' )


# In[12]:


chennai_ambulance = search_requirement('TamilNadu','Chennai','Ambulance')
bangalore_ambulance=search_requirement('Karnataka','Bangalore','Ambulance')
delhi_ambulance=search_requirement('Delhi','Delhi','Ambulance' )


# In[13]:


chennai_Remdesivir = search_requirement('TamilNadu','Chennai','Remdesivir')
bangalore_Remdesivir=search_requirement('Karnataka','Bangalore','Remdesivir')
delhi_Remdesivir=search_requirement('Delhi','Delhi','Remdesivir' )
#delhi_Remdesivir


# In[16]:


chennai_Tocilizumab = search_requirement('TamilNadu','Chennai','Tocilizumab')
bangalore_Tocilizumab=search_requirement('Karnataka','Bangalore','Tocilizumab')
delhi_Tocilizumab=search_requirement('Delhi','Delhi','Tocilizumab' )
#delhi_Tocilizumab


# In[17]:


chennai_blood = search_requirement('TamilNadu','Chennai','blood')
bangalore_blood=search_requirement('Karnataka','Bangalore','blood')
delhi_blood=search_requirement('Delhi','Delhi','blood' )


# In[27]:


import csv
import pandas as pd
frames =[chennai_oxygen_concentrator,bangalore_oxygen_concentrator,delhi_oxygen_concentrator,chennai_oxygen_cylinder,bangalore_oxygen_cylinder,delhi_oxygen_cylinder,chennai_normal_bed,bangalore_normal_bed,delhi_normal_bed,chennai_oxygen_bed,bangalore_oxygen_bed,delhi_oxygen_bed,chennai_icu_bed,bangalore_icu_bed,delhi_icu_bed,chennai_plasma,bangalore_plasma,delhi_plasma,chennai_ventilator,bangalore_ventilator,delhi_ventilator,chennai_ambulance,bangalore_ambulance,delhi_ambulance,chennai_blood,bangalore_blood,delhi_blood,chennai_amphotericin,bangalore_amphotericin,delhi_amphotericin,chennai_Remdesivir,bangalore_Remdesivir,delhi_Remdesivir,chennai_Tocilizumab,bangalore_Tocilizumab,delhi_Tocilizumab]
result = pd.concat(frames)
result.to_excel('twitter op1.xlsx', header=True, index=False)


# In[28]:


"""
List of categories needed:
1.oxygen cylinder
2. Oxygen concentrator
3. General bed #(in twitter normal beds keyword only is available)
4. ICU bed
5. Oxygen bed
6. Ventilator
7. Ambulance
8. Blood
9. Amphotericin
10. Remdesivir
11. Tocilizumab
"""

