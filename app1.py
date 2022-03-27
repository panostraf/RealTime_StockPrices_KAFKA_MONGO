from json import loads
import json
from kafka import KafkaConsumer

dezer = lambda x: loads(x.decode('utf-8'))

# Initialize kafka consumer object
consumer = KafkaConsumer('portfolio',
                        bootstrap_servers=['localhost:9092'],
                        auto_offset_reset='latest',
                        enable_auto_commit=False,
                        group_id='investors',
                        value_deserializer=lambda m: loads(m.decode('utf-8'))
                         )

while True:
    messages = consumer.poll() # kafka obj
    for msg in messages.values():
        for row in msg: # read data
            data = eval(row.value)
            d1 = json.loads(data)

            investor = d1['investor']
            portfolio = d1['portfolio']
            nav = d1['nav']
            change = d1['change']
            change_pct = d1['change_pct']
            dtime = d1['time']

            # create a string for the csv file
            d = f"""{investor},{portfolio},{nav},{change},{change_pct},{dtime}\n"""

            # use investor and portfolio variable to store data in the correct file
            with open(f"{investor}_{portfolio}.csv","a") as f:
                f.write(d)
            print("done")


