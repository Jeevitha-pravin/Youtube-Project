#importing packages

from googleapiclient.discovery import build
import pymysql
import mysql.connector
import pymongo
from pymongo.mongo_client import MongoClient
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
from datetime import datetime

#api connection

def Api_connect():
    Api_Id="AIzaSyBI_Rdp_AGrspUDtptPnRE4vS3NBWkZb24"

    api_service_name="youtube"
    api_version="v3"

    utube=build(api_service_name,api_version,developerKey=Api_Id)
    return utube
youtube=Api_connect()

#sql connection
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="youtube_data"
)
print(mydb)
mycursor = mydb.cursor(buffered=True)


#function to retive channel data from youtube
def channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(channel_name=i["snippet"]["title"],
                channel_id=i["id"],
                subscribtion_count=i["statistics"]["subscriberCount"],
                channel_views=i["statistics"]["viewCount"],
                total_videos=i["statistics"]["videoCount"],
                channel_description=i["snippet"]["description"],
                playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#function to retrive video id data from youtube
def get_videoid(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token= None

    while True:
        response1=youtube.playlistItems().list(
                                            part= 'snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken= next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

def convert_duration(duration_string):
    # By calling timedelta() without any arguments, the duration
    # object is initialized with a duration of 0 days, 0 seconds, and 0 microseconds. Essentially, it sets the initial duration to zero.
    duration_string = duration_string[2:]  # Remove "PT" prefix
    duration = timedelta()
    
    # Extract hours, minutes, and seconds from the duration string
    if 'H' in duration_string:
        hours, duration_string = duration_string.split('H')
        duration += timedelta(hours=int(hours))
    
    if 'M' in duration_string:
        minutes, duration_string = duration_string.split('M')
        duration += timedelta(minutes=int(minutes))
    
    if 'S' in duration_string:
        seconds, duration_string = duration_string.split('S')
        duration += timedelta(seconds=int(seconds))
    
    # Format duration as H:MM:SS
    duration_formatted = str(duration)
    if '.' in duration_formatted:
        hours, rest = duration_formatted.split(':')
        minutes, seconds = rest.split('.')
        duration_formatted = f'{int(hours)}:{int(minutes):02d}:{int(seconds):02d}'
    else:
        duration_formatted = duration_formatted.rjust(8, '0')
    
    return duration_formatted

def convert_timestamp(timestamp):
    datetime_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    formatted_time = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_time


#function to retive video info from youtube API

def get_videoinfo(vi_ids):
    video_data=[]
    for video_id in vi_ids:
        request=youtube.videos().list(
            part="snippet, ContentDetails, statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_ID=item['snippet']['channelId'],
                    video_ID=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_date=convert_timestamp(item['snippet']['publishedAt']),
                    Duration=convert_duration(item['contentDetails']['duration']),
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Fav_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_status=item['contentDetails']['caption']
                )
            video_data.append(data)
    return video_data

#getting comment information

def get_commentinfo(vi_ids):

    Comment_data=[]
    try:
        for video_id in vi_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data

# function to get  PLaylist information from the YouTube API
def get_playlists_details(channel_id):

        next_page_token= None
        playlist_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet, contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token

                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        playlist_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return playlist_data

#upload to mongodb

client=pymongo.MongoClient("mongodb+srv://jeevithasweet1:12345@cluster0.ke5modo.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]

#all function together

def channel_details(channel_id):
    ch_details=channel_info(channel_id)
    vi_ids=get_videoid(channel_id)
    vi_details=get_videoinfo(vi_ids)
    com_details=get_commentinfo(vi_ids)
    pl_details=get_playlists_details(channel_id)


    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details, "playlist_information":pl_details,
                    "video_information":vi_details, "comment_information":com_details})
    return "upload completed successfully"

#sql connection
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
)
print(mydb)
mycursor = mydb.cursor(buffered=True)

mycursor.execute("use youtube_data")
mydb.commit()

#creating channel table

def channel_table():
    #sql connection
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtube_data"
    )
    print(mydb)
    mycursor = mydb.cursor(buffered=True)
    
    mycursor.execute("drop table if exists channel")
    try:
        mycursor.execute('''create table channel(channel_name varchar(100),
                                                        channel_id varchar(80) primary key,
                                                        subscribtion_count bigint,
                                                        channel_views bigint,
                                                        total_videos int,
                                                        channel_description text,
                                                        playlist_id varchar(80))''')
        mydb.commit()
    except:
        print("channel table already created")

    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        sql_query = f"""
            INSERT INTO channel(
                channel_name, channel_id, subscribtion_count, channel_views, total_videos, channel_description, playlist_id
            ) VALUES(%s, %s, %s, %s, %s, %s, %s
            )"""

        sql_values = (row['channel_name'],
                            row['channel_id'],
                            row['subscribtion_count'],
                            row['channel_views'],
                            row['total_videos'],
                            row['channel_description'],
                            row['playlist_id'])
        try:
            mycursor.execute(sql_query, sql_values)
            mydb.commit()
        except:
            print("channel values already inserted")


#creating playlist table
def playlist_table():
    #sql connection
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtube_data"
    )
    print(mydb)
    mycursor = mydb.cursor(buffered=True)

    mycursor.execute("drop table if exists playlist")
    try:
        mycursor.execute('''create table playlist(Playlist_Id varchar(255) primary key,
                                    Title varchar(255),
                                    Channel_Id varchar(100),
                                    Channel_Name varchar(255),
                                    PublishedAt timestamp,
                                    Video_Count int)''')
        mydb.commit()
    except:
        print("playlist table already created")

    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        sql_query = f"""
            INSERT INTO playlist(Playlist_Id,
                                Title,
                                Channel_Id,
                                Channel_Name,
                                PublishedAt,
                                Video_Count)
                                values(%s,%s,%s,%s,%s,%s)"""

        sql_values = (row['Playlist_Id'],
                            row['Title'],
                            row['Channel_Id'],
                            row['Channel_Name'],
                            row['PublishedAt'],
                            row['Video_Count'])
        try:
            mycursor.execute(sql_query, sql_values)
            mydb.commit()
        except:
            print("values inserted successfully") 


def video_table():
    #sql connection
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtube_data"
    )
    print(mydb)
    mycursor = mydb.cursor(buffered=True)

    mycursor.execute("drop table if exists video")
    try:
        mycursor.execute('''create table video(Channel_Name varchar(100),
                            Channel_ID varchar(100),
                            video_ID varchar(100) primary key,
                            Title varchar(150),
                            Tags text,
                            Thumbnail varchar(250),
                            Description text,
                            Published_date timestamp,
                            Duration Time,
                            Views bigint,
                            Likes bigint,
                            Comments int,
                            Fav_count int,
                            Definition varchar(50),
                            Caption_status varchar(50))''')
        mydb.commit()
    except:
        print("video table already created")

    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
        sql_query = f"""
            INSERT INTO video(Channel_Name,
                        Channel_ID,
                        video_ID,
                        Title,
                        Tags,
                        Thumbnail,
                        Description,
                        Published_date,
                        Duration,
                        Views,
                        Likes,
                        Comments,
                        Fav_count,
                        Definition,
                        Caption_status)
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        sql_values = (row['Channel_Name'],
                        row['Channel_ID'],
                        row['video_ID'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Fav_count'],
                        row['Definition'],
                        row['Caption_status'])
        try:

            mycursor.execute(sql_query, sql_values)
            mydb.commit()
        except:
            print("values already inserted")

def comment_table():
    #sql connection
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtube_data"
    )
    print(mydb)
    mycursor = mydb.cursor(buffered=True)  
    mycursor.execute("use youtube_data")

    mycursor.execute("drop table if exists comment")
    try:
        mycursor.execute('''create table comment(Comment_Id varchar(100) primary key,
                                Video_Id varchar(100),
                                Comment_Text text,
                                Comment_Author varchar(100),
                                Comment_published timestamp)''')
        mydb.commit()
    except:
        print("comment table already created")

    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
            sql_query = f"""
                INSERT INTO comment(Comment_Id,
                                Video_Id,
                                Comment_Text,
                                Comment_Author,
                                Comment_published)
                                    values(%s,%s,%s,%s,%s)"""

            sql_values = (row['Comment_Id'],
                                row['Video_Id'],
                                row['Comment_Text'],
                                row['Comment_Author'],
                                row['Comment_published'])
            try:
                mycursor.execute(sql_query, sql_values)
                mydb.commit()
            except:
                print("values inserted successfully")


#all tables
def tables():
    channel_table()
    playlist_table()
    video_table()
    comment_table()

    return "tables created successfully" 

def show_ch_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_pl_table():
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_vi_table():
    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)
    
    return df2

def show_com_table():
    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)
    
    return df3

#streamlit
with st.sidebar:
    choice=option_menu(None,["HOME","EXTRACT DATA AND UPLOAD TO MONGODB","SQL DATA WAREHOUSE","CHANNEL QUERIES"],
                    icons=["HOUSE-DOOR-FILL"],
                    default_index=0,
                    orientation="vertical")
if choice == "HOME":                 
    st.title(":red[YouTube Data Harvesting and Warehousing]")
    st.header("skill take away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("Api Integration")
    st.caption("Data Managenent using MongoDB and Sql")

if choice == "EXTRACT DATA and UPLOAD TO MONGODB":
    st.title(":red[Data Collection and Load Channel Data to MongoDB]")
channel_id=st.text_input("Enter YouTube Channel ID..Get Channel ID From Channel Page")
channels=channel_id.split(',')
channels=[ch.strip() for ch in channels if ch]

if st.button("collect Data"):
    for channel in channels:
        ch_id=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
            ch_id.append(ch_data["channel_information"]["channel_id"])
        if channel_id in ch_id:
            st.success("Given channel_id already exists")
        else:
            insert=channel_details(channel_id)
            st.success(insert)
        

if choice=="SQL DATA WAREHOUSE":
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    )
    print(mydb)
    mycursor = mydb.cursor(buffered=True)

st.title(":red[Data Migration from MongoDB to SQL]")
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT ANY TABLE TO VIEW",("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table=="CHANNELS":
    show_ch_table()

elif show_table=="PLAYLISTS":
    show_pl_table()

elif show_table=="VIDEOS":
    show_vi_table()

elif show_table=="COMMENTS":
    show_com_table()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="youtube_data"
)
print(mydb)
mycursor = mydb.cursor(buffered=True)



Questions=st.selectbox("Select any questions",("1. What are the names of all the videos and their channel name?",
                                               "2. Which channels have the most number of videos, and how many videos they have?",
                                               "3. What are the top 10 most viewed videos and thier respective channels?",
                                               "4. How may comments were made on each video, and what are their video names?",
                                               "5. Which videos have the highest number of likes, what are their channel names?",
                                               "6. What is the total number of likes and dislikes for each video, and what are their video names?",
                                               "7. What is the total number of views for each channel and what are their channel names?",
                                               "8. What are the names of all the channels that have published videos in the year 2022?",
                                               "9. What is the average duration of all videos in each channel, and what are their channel names?",
                                               "10. Which videos have the highest number of comments and what are their channel names?") )
  
if Questions == "1. What are the names of all the videos and their channel name?":
    query1="select Title AS Video_Title, Channel_Name AS Channel_Name FROM video;"
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    st.write(pd.DataFrame(t1,columns=["video_title", "channel_name"]))
                

elif Questions == "2. Which channels have the most number of videos, and how many videos they have?":
    query2="select channel_name as channel_name, total_videos as NO_OF_Videos from channel ORDER BY total_videos DESC;"
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    st.write(pd.DataFrame(t2,columns=["channel_name","no of videos"]))

elif Questions == "3. What are the top 10 most viewed videos and thier respective channels?":
    query3= '''select Views as Views, Channel_Name as channel_name, Title as videotitle from video
                where Views is not null order by Views desc limit 10;'''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    st.write(pd.DataFrame(t3,columns=["Views", "channel_name","video_title" ]))

elif Questions == "4. How may comments were made on each video, and what are their video names?":
    query4= '''select Comments as No_comments ,Title as VideoTitle from video where Comments is not null;'''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    st.write(pd.DataFrame(t4,columns=["no of comments", "video title"]))

elif Questions == "5. Which videos have the highest number of likes, what are their channel names?":
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from video 
                where Likes is not null order by Likes desc;'''
    mycursor.execute(query5)
    mydb.commit()
    t5 = mycursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif Questions == "6. What is the total number of likes and dislikes for each video, and what are their video names?":
       query6 = '''select Likes as likeCount,Title as VideoTitle from video;'''
       mycursor.execute(query6)
       mydb.commit()
       t6 = mycursor.fetchall()
       st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif Questions == "7. What is the total number of views for each channel and what are their channel names?":
    query7 = "select channel_name as ChannelName, channel_views as Channelviews from channel;"
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif Questions == "8. What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select Title as Video_Title, Published_date as published_on, Channel_Name as ChannelName from video 
                where extract(year from Published_date) = 2022;'''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["title", "Video Publised On", "ChannelName"]))

elif Questions == "9. What is the average duration of all videos in each channel, and what are their channel names?":
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM video GROUP BY Channel_Name;"
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    t9=pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append(dict({"Channel Title": channel_title ,  "Average Duration": average_duration_str}))
    df1=(pd.DataFrame(T9))
    st.write(df1)
    
elif Questions == "10. Which videos have the highest number of comments and what are their channel names?":
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from video 
                    where Comments is not null order by Comments desc;'''
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))



    

