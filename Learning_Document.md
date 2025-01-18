# Learnings from Liquid Duck Challenge#


**DuckDB:**	 
DuckDB is an in process, OLAP database for analytics. Its easy to install and run, good for analysing huge amounts of data. Columnar storage makes it faster to read data resulting in faster query performance. Unlike other DBs it does not support distributed storage, its embedded within the application and only one user can write to it at any given time.

*Personal notes:*
-This my first time hearing about duckdb. It was fun learning it from scratch.
-It was easy to install and wto start working on it as it was quite SQL friendly.
-Did not face any major challenges working with duckdb.

**Websockets:**
A Websocket is a communication protocol for that enables persistent duplex commnuincation between client and server.  Without the need of repeated requests the client and server communicate continuously. 

*Personal notes:*
-This is my first time using websockets. Took me a while to learn how the server and client work. Learning the basics of tornado library for Websocket server made it easy to understand and implement the server.
-Websockets and tornado library can be installed using pip.

**Redis:** 
Redis is an in memory database in our case used as a message queue. Since its in memory, it has low latency and can be used for real time processing.

*Personal notes:*
-Redis has stopped it support for windows. It took me good time to figure out how to install redis on windows and use it.
-Use Windows WSL to install Redis and run the Redis on it. By doing so we can access it from vscode.


*Installation Steps:*
	
	Install ubuntu in WSL:
	'''wsl --install -d Ubuntu'''
 
 	Check installation status after its done:
     	'''wsl --list --verbose'''
	Login to ubuntu:

	Install redis:
	'''sudo apt-get update'''
 	'''sudo apt-get install redis-server'''

	Run Redis-server:
	'''redis-server --port 6380'''




**Faker library:**
Used to create a fake data. We can create random names, adreses, email id , contact numbers etc. 
A quick tutorial from chatgpt helped to get started with creating datasets with faker.



