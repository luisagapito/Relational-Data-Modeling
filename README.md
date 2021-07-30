# Sparkify Data Modeling

This data modeling and ETL pipeline aim to allow the analytics team to understand what songs users are listening to.


## Database Schema

The data model consists of a **star schema** that has one fact table and 4 dimension tables. The fact table is the songplays table that has the records in log data associated with song plays. The dimension tables are the users, songs, artists, and time tables. The users table has the information of users in the app. The songs table has the songs in music database. The artists table has the artist in music database. The time table has the timestamps of records in songplays broken down into specific units. This model was chosen because of some key required benefits for this project like fast aggregations, simplified queries, and denormalization.

## ETL Pipeline

The ETL pipeline starts connecting to the sparkify database. Then, it finds the JSON paths from *log_data* and *song_data* directories. Later, processes the *song_data* and writes it to the song and artists tables. After that, the ETL processes the *log_data* and writes it to the song, artist tables. Finally, using all the tables and log_data it is written the songplays fact table. I suggest automatizing this ETL pipeline in a daily batch mode in non-working hours so that the Postgress database is always up to date and the ETL does not consume resources nor compete with queries executed by the analytics team during working hours.

## Database and analytical goals

The main goal is to empower the analytics team will be able to query current data in a fast and optimized manner, letting them gather all the information required to make powerful decisions based on data. For example, they can decide to invest more in a certain type of music in a specific season of the year because they know what songs users are listening to by that time.

## Usage

You can execute the *Project.ipynb* that drops and creates the tables, and execute the ETL pipeline. Most of the scripts have comments for a better understanding of what they are doing.