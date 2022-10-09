# Data Engineering task using Postgresql and Google Big Query 
This repository contains the two python files ETL_script_for_PostgresSQL.py and ETL_script_for_BigQuery.py. Additionally It also contain the sql files for postgresSql, BigQuery and Docx file that contain the result of queries which has been fetch from postgresSql.
* Docker image File is used to containerized both the ETL scripts.
* Elephantsql (https://www.elephantsql.com/) is used which provide PostgresSql database as a service.
* Google BigQuery is also been used which provide fully managed enterprise data warehouse that helps to manage and analyze your data. 
#### Reason of Using the two different databases and setup is just to showcase different skill sets i.e. tradional database, cloud technology, Big data analytics etc.

## Description of the scripts
### ETL_script_for_PostgresSql.py
* This Script fetch the data from two different end points messages and users. Message endpoint doesn't have any nested json however user endpoint has subscription and profile field as a nested json. 
* It normalize the json files and extract subscription array as a different dataframe. But, profile array has been kept in the same user table as there is a 1 to 1 relationship between user and profile.
* Different transformation has been performed like removing PII, anonymization of birthdata, calculation of age, removal of unnecessary columns and messages.
* At last, the three table has been prepared which is User, subscription and messages and loaded into the PostgresSQL. 
* User table has 1 to many relationship with subscription table. 
* User table has two 1 to many relationship with messages table i.e. with receiverid and senderid columns.

### ETL_script_for_BigQuery.py
* This Script also fetch the data from two different end points messages and users. Message endpoint doesn't have any nested json however user endpoint has subscription and profile field as a nested json. 
* In Bigquery we don't normalize the data and therefore we kept the subscription and profile data in the same user table.
* Different transformation has been performed like removing PII, anonymization of birthdata, calculation of age, removal of unnecessary columns and messages.
* At last, the two table has been prepared which is User and messages and loaded into the Bigquery. 
* There is no relation in the table as we don't create a primary and foreign key over here.

### SQL_scripts
* There are two sql script files (one for Postgresql and second for BigQuery) that contain the queries of all the questions that has been asked.
* Additionally there has been a docx files which contains the queries as well as answers to all the question.

## How to Run

* make sure your system have Docker installed otherwise follow the instruction of setup and installation of docker from https://docs.docker.com/docker-for-windows/install/
* You need to create a Postgresql database in elephantsql (https://www.elephantsql.com/). Copy the database conneciton URL and paste in the file ETL_script_for_PostgresSql.py in the load function. If you don't wan't to create your database it will connect with the database that I have created. 
```python
engine = create_engine('YOUR database conneciton URL')
```
* For BigQuery Connection either use the key.json file which will be shared via Email or create your own service account with relevant roles and permission and generate key file. put that key file inside the folder 'Script' and rename it as key.json.

* Open the command terminal 'cmd'
* Move into the directory folder of the project where docker file is present
* Build the docker image using command `docker build . -t sparknetworkchallenge`. The Docker file has been design in a way that it load the data to both the database when you are building the image.
* However, if you want to manually load the data again just start the docker container and move to folder /opt/script.
* once you are in the right folder run each individual file with the command `python3 ETL_script_for_PostgresSql.py` and `python3 ETL_script_for_BigQuery.py`
* --------------- Happy Data Engineering -----------------
