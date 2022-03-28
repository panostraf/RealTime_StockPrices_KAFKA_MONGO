# Setup
Assuming that kafka is installed successfully and properties of zookeeper.service and kafka.service files  has been set, enable zookeeper and kafka services using the following commands
1) sudo systemctl start zookeeper # first start zookeeper
2) sudo systemctl status zookeeper # it should be marked as active
3) sudo systemctl start kafka 
4) sudo systemctl status kafka # should be marked as active
5) sudo systemctl start mongod

# Install requirements
pip install -r requirements.txt

# Start Servers
1) cd in the directory of the project
2) python3 se1_server.py
3) python3 se2_server.py

# Start consumers
1) python3 inv1.py
2) python3 inv2.py
3) python3 inv3.py
#### all of the above files should be printing on terminal p** updated as a sign that the system works correctly
#### consumers are turning into producers and commit data to kafka

# Start consumers of topic portfolios
1) python3 app1.py # it will create csv files for each investor_portfolio
2) python3 app1_mongo.py # it will store data in mongodb: {database:portfolio_db,collection:portfolios}

# Use Spark to query csv files
1) python3 app2.py # it will print on terminal a dataframe with the 20 first rows of the values for the given period
2) python3 app2_mongo.py # It will print on terminal the same results from the mongodb queries
#### On the top of both files change the dates, and it will change the interval of the filter that will be applied on the queries

