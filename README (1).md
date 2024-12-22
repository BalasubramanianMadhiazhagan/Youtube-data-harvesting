
## YouTube Data Harvesting and Warehousing using SQL and Streamlit


- This project is about Streamlit application that allows users to access and analyze data from multiple YouTube channels.

- This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a SQL database, and enables users to search for channel details and join tables to view data in the Streamlit app.
## Installation

Libraries used in this project

- googleapiclient.discovery
- pymongo
- mysql.connector
- pandas 
- streamlit
- datetime
- re

    
## API Reference

   API exploration 

    https://developers.google.com/apis-explorer
   
   API Key 
   
    https://console.cloud.google.com/apis/dashboard




## Features

- "Channel_ID" input by the User.
- "Check for previous enteries and collect and store the Data from the Youtube using 'API' in MongoDB.
- Migrate the retreived Data to SQL through Channel_Name list.
- Using MYSQL the table view of "Channel details", "Video details", "Comment details" got visualized.
- Below 10 queries are used to analyse about and between the channels.

        1.	What are the names of all the videos and their    corresponding channels?

        2.	Which channels have the most number of videos, and how many videos do they have?

        3.	What are the top 10 most viewed videos and their respective channels?

        4.	How many comments were made on each video, and what are their corresponding video names?

        5.	Which videos have the highest number of likes, and what are their corresponding channel names?

        6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?

        7.	What is the total number of views for each channel, and what are their corresponding channel names?

        8.	What are the names of all the channels that have published videos in the year 2022?

        9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?

        10.	Which videos have the highest number of comments, and what are their corresponding channel names?


## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

Channel_ID from ['Youtube'] 
        
        https://www.youtube.com/


## Running Tests

To run tests, run the following link
  
    http://localhost:8501/


