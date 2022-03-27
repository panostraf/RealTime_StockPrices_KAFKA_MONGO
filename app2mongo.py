from pymongo import MongoClient
from datetime import datetime

# Initialize mongo
client = MongoClient()
db = client.portfolio_db
posts = db.portfolios

# Set period filters
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2022,3,31)

# Query for filters and aggregate based on investor and portfolio
query = posts.aggregate([
    {"$match":
         {"time":{"$gte":START_DATE,"$lt":END_DATE}}
     },
    {"$group":
         {"_id":{"investor":"$investor","portfolio":"$portfolio"},
              "min":{"$min":"$nav"},"max":{"$max":"$nav"},
              "avg":{"$avg":"$nav"},
              "std":{"$stdDevPop":"$nav"}}},

    ])

# Print Results
print("From Date:",START_DATE)
print("To Date:",END_DATE)
for doc in query:
    print(doc)