# Cloud Data Warehouse using AWS

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. As their lead data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Datasets

The following two json datasets stored on the cloud (as AWS S3 buckets) are going to be used to create staging tables on Redshift:

### Song Dataset

The song dataset contains information like `artist_id`,`artist_name`,`artist_location`,`song_id`,`title`,`duration`, etc.

### Logs Dataset

The logs dataset contains app data that gives us information about what the users are listening. Information such as `artist`,`auth`,`firstName`,`gender`,`lastName`,`location`,`song`,`timestamp`,`userAgent`,`userId`,etc can be retrieved from this dataset for further analysis.

Using the above two datasets we create two staging tables on Redshift called `staging_events` and `staging_songs`.

## Database Schema

Using data from the staging tables created previously, we now create a star schema optimized for queries on song play analysis. The schema includes the following tables.

### Fact Table

1. __songplays__ 

- This table will contain the records from the log dataset that are associated with song plays. 
- The columns present in this table are `songplay_id`,`start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location` and `user_agent`.

### Dimension Tables

2. __users__

- This table gives information about users in the app. 
- The columns in this table are `user_id`,`first_name`, `last_name`, `gender` and `level`.

3. __songs__

- This table gives information about songs in the music database. 
- The columns in this table are `song_id`, `title`, `artist_id`, `year` and `duration`. 

4. __artists__

- This table gives information about artists in the music database. 
- The columns in this table are `artist_id`, `name`, `location`, `latitude` and `longitude`. 

5. __time__

- This table contains timestamp information from the __songplays__ table broken down into specific units. 
- The columns in this table are `start_time`, `hour`, `day`, `week`, `month`, `year` and `weekday`.

## ETL Pipeline

The SQL queries for creating, dropping and inserting data into tables are stored in the `sql_queries.py` script file as query lists. The `dwh.cfg` file contains credentials for various cloud services and we retrieve credentials using the `configparser` package available in Python. After launching the Redshift cluster and updating credentials we run the `create_tables.py` script that imports query lists from `sql_queries.py`, connects to the Redshift cluster and creates/drops all the tables by running the respective query lists. Once the tables are created successfully, the `etl.py` script needs to be executed in order for it to load data into staging tables from S3 and then from the staging tables, this script runs the insert query lists present in `sql_queries.py` that extract, transform and load data into the analytics tables (fact/dimension tables).

## Testing and Validating the ETL Pipeline

If there are no errors on the terminal after executing all the scripts in an appropriate order, the Query Editor available on the Redshift console can be used to run sample queries on the analytics tables. 
