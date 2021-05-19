import configparser
import json
import asyncio
import csv
import pandas as pd
from datetime import date, datetime

from datetime import datetime as dt

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import (GetHistoryRequest)
from telethon.tl.types import (
    PeerChannel
)


# some functions to parse json date
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)


# Reading Configs
# config = configparser.ConfigParser()
# config.read("config.ini")

# Setting configuration values
api_id = '5268989'
api_hash = '144d13022714245747452500b737a1c1'

api_hash = str(api_hash)

phone = '+91 8297974466'
username = '@akilesh22'

# Create the client and connect
client = TelegramClient(username, api_id, api_hash)

async def main(phone):
    await client.start()
    print("Client Created")
    # Ensure you're authorized
    if await client.is_user_authorized() == False:
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    me = await client.get_me()

    # x = ['https://t.me/CovidNCR', 'https://t.me/CovidBengaluru', 'https://t.me/CovidHyderabad']
    # user_input_channel = input('enter entity(telegram URL or entity id):')
    user_input_channel = 'https://t.me/CovidNCR'

    if user_input_channel.isdigit():
        entity = PeerChannel(int(user_input_channel))
    else:
        entity = user_input_channel

    my_channel = await client.get_entity(entity)

    offset_id = 0
    limit = 100
    all_messages = []
    total_messages = 0
    total_count_limit = 1000

    while True:
        print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
        history = await client(GetHistoryRequest(
            peer=my_channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            all_messages.append(message.to_dict())
        offset_id = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    df = pd.DataFrame.from_dict(all_messages, orient='columns')
    df.to_csv('delhi.csv')



with client:
    client.loop.run_until_complete(main(phone))

df = pd.read_csv('hyderabad.csv')

df['user_id'] = df.from_id.str.extract('(\d+)')
df['date'] = df.date.str.replace('00:00', '')
df['date'] = df.date.str.replace('+', '')

df['message'] = df['message'].fillna('').astype(str)
df_grouped = df.groupby(['user_id', 'date'])['message'].sum()
df1 = df_grouped.to_frame()
df1[['state', 'city']] = ['Delhi', 'NCR']
df1.to_excel('delhi_grouped.xlsx')