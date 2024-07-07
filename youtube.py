import googleapiclient.discovery
import pymongo
import mysql.connector
from pymongo import MongoClient
import pandas as pd
import streamlit as st
import datetime
import re
from googleapiclient.discovery import build

# API key connection
def API_connect():
    api_service_name = "youtube"
    api_version = "v3"
    api_key="AIzaSyAcj3h4yf1uU2nddkRjnY4ld6kgred7NtE"
    youtube = build(api_service_name, api_version,developerKey=api_key)
    
    return youtube

youtube=API_connect()

# To get channel details

def Channel_data(channel_id):
    request = youtube.channels().list(
        part="contentDetails,snippet,statistics,status",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data={'channel_name':i['snippet']['title'],
                    'channel_id':i['id'],
                    'channel_description':i['snippet']['description'],
                    'channel_SubscriberCount':i['statistics']['subscriberCount'],
                    'channel_ViewCount':i['statistics']['viewCount'],
                    'channel_videoCount':i['statistics']['videoCount'],
                    'channel_status':i['status']['privacyStatus'],
                    'Playlist_id':i['contentDetails']['relatedPlaylists']['uploads']}
    return data

# to get the video_id

def get_video_ids(channel_id):
        Video_ids=[]
        
        response = youtube.channels().list( id=channel_id,part="contentDetails").execute()
        playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token=None

        while True:
                response1 = youtube.playlistItems().list(
                        part="snippet",
                        playlistId=playlist_Id,
                        maxResults=50,
                        pageToken=next_page_token).execute()
                for i in range(len(response1['items'])):
                        Video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                next_page_token=response1.get('nextPageToken')
                
                if next_page_token is None:
                        break
        return Video_ids
    
    
# to get video information

def Video_info(Video_ids):
    Video_data=[]
    for video_id in Video_ids:
        request = youtube.videos().list(
            part="contentDetails,snippet,statistics",
            id=video_id)
        response2 = request.execute()
        
        for item in response2['items']:
            youtube_datetime_str = item['snippet']['publishedAt']
            youtube_datetime = datetime.datetime.strptime(youtube_datetime_str, '%Y-%m-%dT%H:%M:%SZ')
            item['snippet']['publishedAt'] = youtube_datetime.strftime('%Y-%m-%d %H:%M:%S')
            
            duration_str = item['contentDetails']['duration']
            duration_pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
            match = duration_pattern.match(duration_str)

            if match:
                hours = int(match.group(1) or 0)
                minutes = int(match.group(2) or 0)
                seconds = int(match.group(3) or 0)

                formatted_duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                item['contentDetails']['duration'] = formatted_duration
            
            data=dict(channel_name=item['snippet']['channelTitle'],
                    channel_Id=item['snippet']['channelId'],
                    Video_id=item['id'],
                    Video_name=item['snippet']['title'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics']['viewCount'],
                    Likes=item['statistics'].get('likeCount'),
                    FavoriteCount=item['statistics']['favoriteCount'],
                    CommentCount=item['statistics'].get('commentCount'),
                    Caption_status=item['contentDetails']['caption']
                    )
            Video_data.append(data)
    return Video_data


# Comment information

def Comment_info(Video_ids):
    Comment_data=[]
    try:
        for video_id in Video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50)
            response3 = request.execute()               

            for item in response3['items']:
                youtube_datetime_str = item['snippet']['topLevelComment']['snippet']['publishedAt']
                youtube_datetime = datetime.datetime.strptime(youtube_datetime_str, '%Y-%m-%dT%H:%M:%SZ')
                item['snippet']['topLevelComment']['snippet']['publishedAt'] = youtube_datetime.strftime('%Y-%m-%d %H:%M:%S')
                data=dict(
                    Comment_id=item['id'],
                    video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_txt=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_PublishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)

    except:
        pass
    return Comment_data


# MongoDB connection

client=MongoClient('localhost',27017)
db=client["youtube_data"]


def Channel_details(channel_id):
    Ch_details=Channel_data(channel_id)
    Video_IDs=get_video_ids(channel_id)
    Vd_details=Video_info(Video_IDs)
    Cmnt_details=Comment_info(Video_IDs)

    collection1=db["Channel_details"]
    collection1.insert_one({"channel_details":Ch_details,"videos_details":Vd_details,"comment_details":Cmnt_details})
    return "Uploaded successfully"


# Channel table creation through MySQL
def channels_table(channel_name_1):
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user= "root",
        password= "bala26554565",
        database="youtube_project",
        auth_plugin="mysql_native_password")
    cursor=conn.cursor()

    
    create_query="""create table if not exists Channel(
                                                channel_id varchar (80) primary key,
                                                channel_name varchar (250),
                                                channel_description text,
                                                channel_SubscriberCount int,
                                                channel_videoCount bigint,
                                                channel_ViewCount int,
                                                channel_status varchar (100),
                                                Playlist_id varchar (80))"""
    cursor.execute(create_query)
    conn.commit()
    
        

    Single_channel_name=[]  
    db = client["youtube_data"]
    collection1=db["Channel_details"]
    for ch_data in collection1.find({"channel_details.channel_name": channel_name_1},{"_id":0}):
        Single_channel_name.append(ch_data["channel_details"])
        
    df_SC_detail=pd.DataFrame(Single_channel_name)

    for index,row in df_SC_detail.iterrows():
        insert_query="""insert into Channel(channel_name,
                                            channel_id,
                                            channel_description,
                                            channel_SubscriberCount,
                                            channel_videoCount,
                                            channel_ViewCount,
                                            channel_status,
                                            Playlist_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s)"""
        values=(row['channel_name'],
                row['channel_id'],
                row['channel_description'],
                row['channel_SubscriberCount'],
                row['channel_videoCount'],
                row['channel_ViewCount'],
                row['channel_status'],
                row['Playlist_id'])
        try:
            cursor.execute(insert_query,values)
            conn.commit()
        except:
            news=f"Channel name {channel_name_1} already exists"
            return news



# Videos table creation through MySQL
def videos_table(channel_name_1):
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user= "root",
        password= "bala26554565",
        database="youtube_project",
        auth_plugin="mysql_native_password")
    cursor=conn.cursor()

    
    create_query="""create table if not exists Videos(
                                                        channel_name varchar(100),
                                                        channel_Id varchar(100),
                                                        Video_id varchar(30) primary key,
                                                        Video_name varchar(100),
                                                        Thumbnail varchar(200),
                                                        PublishedAt timestamp,
                                                        Duration VARCHAR(20),
                                                        Views BIGINT,
                                                        Likes BIGINT,
                                                        FavoriteCount BIGINT,
                                                        CommentCount BIGINT,
                                                        Caption_status varchar(30))"""
    cursor.execute(create_query)
    conn.commit()
   
        
    single_video_details=[] 
    db = client["youtube_data"]
    collection1=db["Channel_details"]
    for ch_data in collection1.find({"channel_details.channel_name":channel_name_1},{"_id":0}):
        single_video_details.append(ch_data["videos_details"])
    df_SV_detail=pd.DataFrame(single_video_details[0])



    for index,row in df_SV_detail.iterrows():
        insert_query="""insert into Videos(channel_name,
                                        channel_Id,
                                        Video_id,
                                        Video_name,
                                        Thumbnail,
                                        PublishedAt,
                                        Duration,
                                        Views,
                                        Likes,
                                        FavoriteCount,
                                        CommentCount,
                                        Caption_status)
                                            
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        values=(row['channel_name'],
                row['channel_Id'],
                row['Video_id'],
                row['Video_name'],
                row['Thumbnail'],
                row['PublishedAt'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['FavoriteCount'],
                row['CommentCount'],
                row['Caption_status']
                )
        try:
            cursor.execute(insert_query,values)
            conn.commit()
        except:
            print('values already inserted')
    
    
# Comments table creation through MySQL

def comments_table(channel_name_1):
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user= "root",
        password= "bala26554565",
        database="youtube_project",
        auth_plugin="mysql_native_password")
    cursor=conn.cursor()

    create_query="""create table if not exists Comments(
                                                        Comment_id varchar(30),
                                                        video_id varchar(30),
                                                        Comment_txt text,
                                                        Comment_Author varchar(100),
                                                        Comment_PublishedAt timestamp                                                        
                                                        )"""
    cursor.execute(create_query)
    conn.commit()

    single_comment_details=[] 
    db = client["youtube_data"]
    collection1=db["Channel_details"]
    for ch_data in collection1.find({"channel_details.channel_name": channel_name_1},{"_id":0}):
        single_comment_details.append(ch_data["comment_details"])
    df_C_detail=pd.DataFrame(single_comment_details[0])

    for index,row in df_C_detail.iterrows():
        insert_query="""insert into Comments(Comment_id,
                                            video_id,
                                            Comment_txt ,
                                            Comment_Author,
                                            Comment_PublishedAt)
                                            
                                        values(%s,%s,%s,%s,%s)"""
        values=(row['Comment_id'],
                row['video_id'],
                row['Comment_txt'],
                row['Comment_Author'],
                row['Comment_PublishedAt'],
                )
        try:
            cursor.execute(insert_query,values)
            conn.commit()
        except:
            print('values already inserted')
    
# Tables creation and Display
 
def tables(S_channel):
    news= channels_table(S_channel)
    if news:
        return news
    else:
        videos_table(S_channel)
        comments_table(S_channel)
        return "Tables created successfully"


def Show_channel_table():
    ch_list=[]  
    db = client["youtube_data"]
    collection1=db["Channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_details":1}):
        ch_list.append(ch_data['channel_details'])
    df=st.dataframe(ch_list)
    
    return df


def Show_video_table():
    vd_list=[]  
    db = client["youtube_data"]
    collection1=db["Channel_details"]
    for vd_data in collection1.find({},{"_id":0,"videos_details":1}):
        for i in range(len(vd_data["videos_details"])):
            vd_list.append(vd_data["videos_details"][i])
    df2=st.dataframe(vd_list)
    
    return df2

def Show_comment_table():
    cmt_list=[]  
    db = client["youtube_data"]
    collection1=db["Channel_details"]
    for cmt_data in collection1.find({},{"_id":0,"comment_details":1}):
        for i in range(len(cmt_data["comment_details"])):
            cmt_list.append(cmt_data["comment_details"][i])
    df3=st.dataframe(cmt_list)
    
    return df3


# Streamlit processing

with st.sidebar:
    st.title(":red[Youtube Data Harvesting & Warehousing]")
    st.selectbox("Skill Take Away",("Check the drop down",
                                    "Python Scripting",
                                    "Data collection",
                                    "MongoDB",
                                    "API Integration",
                                    "Data Management Using MongoDB and SQL"))
    st.header("Functions")
    st.caption("Compare between channels")
    st.caption("Check likes")
    st.caption("Check views")
    st.caption("Check comments")
    st.caption("check average time duration of videos")
    
channel_id=st.text_input("Enter the channel ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_details":1}):
        ch_ids.append(ch_data['channel_details']["channel_id"])
    try:
        if channel_id in ch_ids:
            st.success("Channel ID exist already")
            
        else:
            insert=Channel_details(channel_id)
            st.success(insert)
            st.balloons()
    except:
        st.error("Enter a valid channel ID")        
        
All_channels=[]
db = client["youtube_data"]
collection1=db["Channel_details"]
for ch_data in collection1.find({},{"_id":0,"channel_details":1}):
    All_channels.append(ch_data['channel_details']['channel_name'])

unique_channel=st.selectbox("Select the channel",All_channels)
        
if st.button("Migrate to SQL"):
    Table=tables(unique_channel)
    st.success(Table)
    st.balloons()
    
Show_table=st.radio("Select the table for view",("Channels","Videos","Comments"))

if Show_table=="Channels":
    Show_channel_table()
    
if Show_table=="Videos":
    Show_video_table()

if Show_table=="Comments":
    Show_comment_table()
    
    
# SQL connection
conn = mysql.connector.connect(
        host="127.0.0.1",
        user= "root",
        password= "bala26554565",
        database="youtube_project",
        auth_plugin="mysql_native_password")
cursor=conn.cursor()

Question=st.selectbox("Select your Query",("1.The names of all the videos and their corresponding channels",
                                           "2. Channels have the most number of videos and their videos count",
                                           "3. The top 10 most viewed videos and their respective channels",
                                           "4. Total comments with respective videos",
                                           "5. Videos with highest number of likes with channel",
                                           "6. Total number of likes with respective videos",
                                           "7. Total number of views with respective channel",
                                           "8. Videos published in the year 2022",
                                           "9. Average duration(minutes) of all videos in each channel",
                                           "10. Videos with highest number of comments")
                      )

if Question == "1.The names of all the videos and their corresponding channels":
        Query1='''Select Video_name as videos,channel_name as channelname from videos'''
        cursor.execute(Query1)
        t1=cursor.fetchall()
        df=pd.DataFrame(t1,columns=["Video title","Channel name"])
        st.write(df)

elif Question == "2. Channels have the most number of videos and their videos count":
        Query2 ='''Select channel_name as channelname,channel_videoCount as Total_videos from channel
                        order by channel_videoCount desc'''
        cursor.execute(Query2)
        t2=cursor.fetchall()
        df2=pd.DataFrame(t2,columns=["Channel name","Total videos"])
        st.write(df2)
        
elif Question == "3. The top 10 most viewed videos and their respective channels":
        Query3 ='''Select views as views,channel_name as Channelname,Video_name as video_title from videos
                        where views is not null order by views desc limit 10'''
        cursor.execute(Query3)
        # conn.commit()
        t3=cursor.fetchall()
        df3=pd.DataFrame(t3,columns=["Views","Channel name","Video_Name"])
        st.write(df3)
        
elif Question == "4. Total comments with respective videos":
        Query4 ='''Select CommentCount as Totalcomments,Video_name as video_title from videos
                        where CommentCount is not null'''
        cursor.execute(Query4)
        # conn.commit()
        t4=cursor.fetchall()
        df4=pd.DataFrame(t4,columns=["Total comments","Video title"])
        st.write(df4)
        
elif Question == "5. Videos with highest number of likes with channel":
        Query5 ='''Select Video_name as video_title,Likes as Total_likes,channel_name as Channelname from videos
                        where Likes is not null order by likes desc'''
        cursor.execute(Query5)
        # conn.commit()
        t5=cursor.fetchall()
        df5=pd.DataFrame(t5,columns=["Video title","likes count","Channel name"])
        st.write(df5)
        
elif Question == "6. Total number of likes with respective videos":
        Query6 ='''Select Likes as Total_likes,Video_name as video_title from videos'''
        cursor.execute(Query6)
        # conn.commit()
        t6=cursor.fetchall()
        df6=pd.DataFrame(t6,columns=["likes count","Video title"])
        st.write(df6)
        
elif Question == "7. Total number of views with respective channel":
        Query7 ='''Select channel_name as Channelname,channel_ViewCount as Total_views from channel'''
        cursor.execute(Query7)
        # conn.commit()
        t7=cursor.fetchall()
        df7=pd.DataFrame(t7,columns=["Channel name","Views count"])
        st.write(df7)
        
elif Question == "8. Videos published in the year 2022":
        Query8 ='''Select Video_name as video_title,PublishedAt as Date_of_release,channel_name as Channelname from videos
                   where extract(year from PublishedAt)=2022 '''
        cursor.execute(Query8)
        # conn.commit()
        t8=cursor.fetchall()
        df8=pd.DataFrame(t8,columns=["Video title","Published date","Channel name"])
        st.write(df8)
        
elif Question == "9. Average duration(minutes) of all videos in each channel":
        Query9 ='''Select channel_name as Channelname, AVG(TIME_TO_SEC(Duration)) / 60 AS average_duration_minutes from videos group by channel_name'''
        cursor.execute(Query9)
        # conn.commit()
        t9=cursor.fetchall()
        df9=pd.DataFrame(t9,columns=["Channel name","Average duration"])

        T9=[]
        for index,row in df9.iterrows():
                channel_title=row["Channel name"]
                averageduration=row["Average duration"]
                Average_duration=str(averageduration)
                T9.append(dict(Channeltitle=channel_title,AVG_duration=Average_duration))
        df1=pd.DataFrame(T9)
        st.write(df1)
        
elif Question == "10. Videos with highest number of comments":
        Query10 ='''Select Video_name as video_title,channel_name as Channelname,CommentCount as Totalcomments from videos
                        where CommentCount is not null order by CommentCount desc'''
        cursor.execute(Query10)
        # conn.commit()
        t10=cursor.fetchall()
        df10=pd.DataFrame(t10,columns=["Video title","Channel name","Total comments"])
        st.write(df10)