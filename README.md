### YouTube Data Harvesting and Warehousing using SQL and Streamlit###


#Purpose#

This project is about Streamlit application that allows users to access and analyze data from multiple YouTube channels.

#Applications and software used#

  *Python Scripting*,
  *Data collection*,
  *MongoDB*,
  *API Integration*,
  *Data Management Using MongoDB and SQL*
  
##Desription##

This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, 
stores it in a SQL database, and enables users to search for channel details and join tables to view data in the Streamlit app.

##Procedure & functions##

1.Used google API by creating unique API key for extracting Datas of different channel IDs for the "Yout tube" using API Integration.

2 Used python scripting completely based on Function blocks containing 'For loops' to retrieve the datas.

3.The retrieved data got stored in the seperate database using MongDB.

4.From the Mongo, the collection data got processed in the *MySQl* software tool for Table creation and manipulation.

5.Applied 10 queries to view the datas based on user interest such as given below,

    *A.The names of all the videos and their corresponding channels*,
    *B. Channels have the most number of videos and their videos count*,
    *C. The top 10 most viewed videos and their respective channels*,
    *D. Total comments with respective videos*,
    *E. Videos with highest number of likes with channel*,
    *F. Total number of likes with respective videos*,
    *G. Total number of views with respective channel*,
    *H. Videos published in the year 2022*,
    *I. Average duration(minutes) of all videos in each channel*,
    *J. Videos with highest number of comments*
    
6.Used streamlit application to view and display the manipulated data.

