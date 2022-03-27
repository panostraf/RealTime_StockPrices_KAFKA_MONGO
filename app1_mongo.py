import datetime
from json import loads
import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# Initialize mongo db connection in specific db and collection
client = MongoClient()
db = client.portfolio_db
posts = db.portfolios


dezer = lambda x: loads(x.decode('utf-8'))

# Initialize a kafka consumer object
consumer = KafkaConsumer('portfolio',
                        bootstrap_servers=['localhost:9092'],
                        auto_offset_reset='latest',
                        enable_auto_commit=False,
                        group_id='investorsMongo',
                        value_deserializer=lambda m: loads(m.decode('utf-8'))
                         )


while True:
    messages = consumer.poll() # kafka obj
    for msg in messages.values():
        for row in msg: # read data
            data = eval(row.value)
            d1 = json.loads(data)

            d1["time"] = datetime.datetime.now() #get current time in a form acceptable by mongo
            print(d1)
            post_id = posts.insert_one(d1).inserted_id # push data to mongo db
            print("done")
