from pymongo import MongoClient
from datetime import datetime

client = MongoClient()

db = client.portfolio_db
posts = db.portfolios

query = """aggregate({$group: {_id:"$investor", "min":{"$min":"$nav"},"max":{"$max":"$nav"},"avg":{"$avg":"$nav"}}})"""
for x in posts.aggregate([{"$match":{"time":{"$gte":datetime(2022, 1, 1),"$lt":datetime(2022,4,1)}}},{"$group": {"_id":{"investor":"$investor","portfolio":"$portfolio"}, "min":{"$min":"$nav"},"max":{"$max":"$nav"},"avg":{"$avg":"$nav"}}}]):
    print(x)