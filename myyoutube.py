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

def Api_connect():
    Api_Id="AIzaSyB4dQycDnL7v28PoJ4ZKyQgd4-jc3o0JpE"

    api_service_name="youtube"
    api_version="v3"

    utube=build(api_service_name,api_version,developerKey=Api_Id)
    return utube
youtube=Api_connect()

#function to retrive channel data from youtube

def get_channel_data(channel_id):
    response = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id
    ).execute()
    #store the desired channel data which are extracted from youtube into a dictionary
    for i in response["items"]:
        channel_data={'Channel_name': i['snippet']["title"],
            'Channel_id':i["id"],
            'Subscribers' : i['statistics']['subscriberCount'],
            'Views':i['statistics']['viewCount'],
            'Total_Videos' : i['statistics']['videoCount'],
            'channel_description':i['snippet']['description'],
            'Playlist_id' : i['contentDetails']['relatedPlaylists']['uploads']}
    return channel_data

#function to retrive playlist info from youtube API

def get_video_ids(channel_id):
    response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
        ).execute()
        
    playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    video_id=[]
    next_page_token = None
    while True:
        id_response = youtube.playlistItems().list(
        part = "snippet",
        playlistId = playlist_Id,
        maxResults=50,
        pageToken = next_page_token
        ).execute()
        for id in (id_response['items']):
            video_id.append(id['snippet']['resourceId']['videoId'])
            next_page_token = id_response.get('nextPageToken')
        if next_page_token is None:
            break
    return video_id

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

#function to retrive video info from youtube API

def get_video_data(Video_id):
    videodata=[]
    for videoid in Video_id:
        video_data_response=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=videoid
        ).execute()
        for vd in video_data_response['items']:
            videodata.append({'video_name': vd['snippet']["title"],
                            'video_id':vd['id'],
                            'channel_name':vd['snippet']['channelTitle'],
                            'channel_id':vd['snippet']['channelId'],
                            'Published_date':convert_timestamp(vd['snippet']['publishedAt']),
                            'video_description':vd['snippet']['description'],
                            'Views' : vd['statistics'].get('viewCount'),
                            'Likes' : vd['statistics'].get('likeCount'),
                            'Fav_count':vd['statistics']['favoriteCount'],
                            'Comment_Count': vd['statistics'].get('commentCount'),
                            'Duration': convert_duration(vd['contentDetails']['duration']),
                            'Thumbnail':vd['snippet']['thumbnails']['default']['url'],
                            'Caption_status':vd['contentDetails']['caption']
                            })
    return videodata

#function to retrive comment info from youtube API

def get_comment_details(Video_id):
    comment_data=[]
    try:
        for video in Video_id:
            comment_response = youtube.commentThreads().list(
                part="snippet",
                videoId=video,
                maxResults=10
                ).execute()
            for cm in comment_response['items']:
                info ={'Comment_id':cm['snippet']['topLevelComment']['id'],
                    'videoid':cm['snippet']['topLevelComment']['snippet']['videoId'],
                    'comment_author' :cm['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_text':cm['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_publishedDate':convert_timestamp(cm['snippet']['topLevelComment']['snippet']['publishedAt'])
                    }
                                
                comment_data.append(info)
    except:
        pass
    return comment_data

# function to get  PLaylist information from the YouTube API
def get_playlist_data(channel_id):
        # Retrieve the PLaylist information from the YouTube API

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
                        data=dict(Playlist_id=item['id'],
                                Channel_id=item['snippet']['channelId'],
                                Playlist_title=item['snippet']['title'])
                        playlist_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return playlist_data
    
#upload to mongodb

client=pymongo.MongoClient("mongodb+srv://jeevithasweet1:12345@cluster0.ke5modo.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]
coll1=db.channel_details

#insert data into mongoDB
def get_all_details(channel_id):
    Channel_data=get_channel_data(channel_id)
    Playlist_data=get_playlist_data(channel_id)
    Video_id=get_video_ids(channel_id)
    Video_data=get_video_data(Video_id)
    Comment_data=get_comment_details(Video_id)
    
    data_file=({"Channel_Details":Channel_data,"Playlist_details":Playlist_data,"Video_data":Video_data,"Comments":Comment_data})
    
    return data_file

def upload_to_mongoDB(data_file):
    client=pymongo.MongoClient("mongodb+srv://jeevithasweet1:12345@cluster0.ke5modo.mongodb.net/?retryWrites=true&w=majority")
    db=client["youtube_data"]
    coll1=db["channel_details"]
    coll1.insert_one(data_file)
    

#sql connection

import mysql.connector
mydb = mysql.connector.connect(
host="localhost",
user="root",
password="",
#database='joins'
)
print(mydb)
mycursor = mydb.cursor(buffered=True)

mycursor.execute("create database if not exists youtube_data")
mydb.commit()

mycursor.execute("use youtube_data")
mydb.commit()


def create_table():

    create_channeltable='''CREATE TABLE IF NOT EXISTS channels_data
                            (Channel_name VARCHAR(100),
                            Channel_id VARCHAR(100) PRIMARY KEY,
                            Subscribers BIGINT,
                            Views BIGINT,
                            Total_videos BIGINT,
                            Channel_description TEXT,
                            Playlist_id VARCHAR(100))'''
        
    mycursor.execute(create_channeltable)
    mydb.commit()

    create_playlist_table='''CREATE TABLE IF NOT EXISTS playlist_data(
                                Playlist_id VARCHAR(50) PRIMARY KEY,
                                Channel_id VARCHAR(100),
                                Playlist_title VARCHAR(100))'''
    mycursor.execute(create_playlist_table)
    mydb.commit()

    create_videotable = '''
                CREATE TABLE IF NOT EXISTS video_details (
                video_name VARCHAR(500),
                video_id VARCHAR(255) PRIMARY KEY,
                channel_name varchar(100),
                channel_id VARCHAR(500),
                Published_date timestamp,
                video_description TEXT, 
                Views BIGINT,
                Likes BIGINT,
                Fav_count int,
                Comment_Count INT,
                Duration Time,
                Thumbnail varchar(250),
                Caption_status varchar(50))'''
    mycursor.execute(create_videotable)
    mydb.commit()

    create_comment_table = '''
                    CREATE TABLE IF NOT EXISTS comment_details (
                    Comment_id VARCHAR(100) PRIMARY KEY,
                    videoid VARCHAR(255),
                    comment_author TEXT,
                    comment_text TEXT,
                    comment_publishedDate DATE)'''
            
    mycursor.execute(create_comment_table)
    mydb.commit()
    
    return True

#migrate data from mongodb to sql
def insert_into_sql():
    
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    #database='joins'
    )
    print(mydb)
    mycursor = mydb.cursor(buffered=True)
    
    mycursor.execute("use youtube_data")
    mydb.commit()
    
    #upload to mongodb

    client=pymongo.MongoClient("mongodb+srv://jeevithasweet1:12345@cluster0.ke5modo.mongodb.net/?retryWrites=true&w=majority")
    db=client["youtube_data"]
    coll1=db["channel_details"]
    
    # Inserting channel details

    ch_details=[]
    for data in coll1.find({},{"_id":0,'Channel_Details':1}):
        ch_details.append(data['Channel_Details'])
        df=pd.DataFrame(ch_details)
        for index,row in df.iterrows(): 
        #print(index,row)
            insert_channeldetails = '''INSERT IGNORE INTO channels_data (Channel_name,Channel_id,Subscribers,Views,Total_videos,Channel_description,Playlist_id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''

            values=(row['Channel_name'],row['Channel_id'],row['Subscribers'],row['Views'],row['Total_Videos'],row['channel_description'],row['Playlist_id'])
            mycursor.execute(insert_channeldetails, values)
            mydb.commit()
            
#Inserting Playlist_details

    pl_details=[]
    for data in coll1.find({},{'Playlist_details':1}):
        for i in range(len(data['Playlist_details'])):
            pl_details.append(data['Playlist_details'][i])
            df1=pd.DataFrame(pl_details)
    for index,row in df1.iterrows():
        #print(index,row)
        insert_playlistdetails='''INSERT IGNORE INTO playlist_data(Playlist_id,Channel_id,Playlist_title)
                                    values(%s,%s,%s)'''
            
        values =(row['Playlist_id'],row['Channel_id'],row['Playlist_title'])
        mycursor.execute( insert_playlistdetails,values)
        mydb.commit()
        
# Inserting video details

    video_dat = []   
    for data in coll1.find({},{"_id":0,'Video_data':1}):
        for j in range(len(data['Video_data'])):
            video_dat.append(data['Video_data'][j])
            df2=pd.DataFrame(video_dat)
    for index,row in df2.iterrows():
        insert_videodetails = '''INSERT IGNORE INTO video_details (
        video_name, video_id, channel_name, channel_id, Published_date, video_description, Views, Likes, Fav_count, Comment_Count, Duration, Thumbnail, Caption_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values =( row['video_name'],row['video_id'],row['channel_name'],row['channel_id'],row['Published_date'],row['video_description'],row['Views'],row['Likes'],row['Fav_count'],row['Comment_Count'],row['Duration'],row['Thumbnail'],row['Caption_status'])
        mycursor.execute(insert_videodetails, values)
        mydb.commit()
        
# Inserting comment details

    comment_dat = []    
    for data in coll1.find({},{"_id":0,'Comments':1}):
        for k in range(len(data['Comments'])):
            comment_dat.append(data['Comments'][k])
            df3=pd.DataFrame(comment_dat)  
    for index,row in df3.iterrows():
        #print(index,row)
        insert_commentdetails='''INSERT IGNORE INTO comment_details(Comment_id, videoid, comment_author, comment_text, comment_publishedDate )
                                    values(%s,%s,%s,%s,%s)'''
        
        values =(row['Comment_id'],row['videoid'],row['comment_author'],row['comment_text'],row['comment_publishedDate'])
        mycursor.execute(insert_commentdetails,values)
        mydb.commit()

client=pymongo.MongoClient("mongodb+srv://jeevithasweet1:12345@cluster0.ke5modo.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]
coll1=db["channel_details"]

        
def show_channel_table():
    ch_details=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for data in coll1.find({},{"_id":0,'Channel_Details':1}):
            ch_details.append(data['Channel_Details'])
    df5=st.dataframe(ch_details )
    return df5
    
def show_playlist_table():
    pl_details=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for data in coll1.find({},{'Playlist_details':1}):
        for i in range(len(data['Playlist_details'])):
            pl_details.append(data['Playlist_details'][i])
    df6=st.dataframe(pl_details)
    return df6

def show_video_table():
    video_dat = []
    db=client["youtube_data"]
    coll1=db["channel_details"] 
    for data in coll1.find({},{"_id":0,'Video_data':1}):
        for j in range(len(data['Video_data'])):
            video_dat.append(data['Video_data'][j])
    df7=st.dataframe(video_dat)
    return df7

def show_comment_table():
    comment_dat = []
    db=client["youtube_data"]
    coll1=db["channel_details"]    
    for data in coll1.find({},{"_id":0,'Comments':1}):
        for k in range(len(data['Comments'])):
            comment_dat.append(data['Comments'][k])
    df8=st.dataframe(comment_dat)
    return df8


#streamlit

mydb = mysql.connector.connect(
host="localhost",
user="root",
password="",
database="youtube_data"
)
print(mydb)
mycursor = mydb.cursor(buffered=True)

mycursor.execute("use youtube_data")
mydb.commit()


# Configuring Streamlit GUi

st.set_page_config(layout='wide')

base="dark"

# Title
st.title(':red[Youtube Data Harvesting]')

# Data collection zone
col1, col2 = st.columns(2)
with col1:
    st.header(':white[EXTRACT DATA AND UPLOAD TO MONGODB]')
    st.write ('(Note:- Here we collecting data by using channel id and stored it in **MongoDB database**.)')
    
    channel_id = st.text_input('**Enter Youtube channel_id**')   
    if st.button('**COLLECT DATA**'):
        ch_id=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for channel_details in coll1.find({},{"_id":0, "Channel_Details":1}):
            ch_id.append(channel_details["Channel_Details"]["Channel_id"])
        if channel_id in ch_id:
            st.success("Given channel_id already exists")
        else:
            #all_data=get_all_details(channel_id)
            st.success("DATA COLLECTED SUCCESSFULLY")
            
    if st.button("Store TO MongoDB"):
        data_file=get_all_details(channel_id)
        upload_to_mongoDB(data_file)

with col2:
    st.header(':white[MIGRATE FROM MONGODB TO SQL]')
    st.write ('''(Note:- Here you migrate the specific youtube channel from mongodb to sql)''')
    if st.button("Migrate to SQL"):
        create_table()
        insert_into_sql()
        st.write("Migrated Successfully")
    
show_table=st.radio("SELECT ANY TABLE TO VIEW",("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"), horizontal= True)

if show_table=="CHANNELS":
    show_channel_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_video_table()

elif show_table=="COMMENTS":
    show_comment_table()
    
st.header(':white[Channel Data Analysis]')
st.write ('''(Note:- You can analyse the channel information depends on your question selection, you can see a table format output.)''')


question = st.selectbox('Select your Queries',
                                ('1.What are the names of all the videos and their corresponding channels?',
                                '2.Which channels have the most number of videos, and how many videos do they have?',
                                '3.What are the top 10 most viewed videos and their respective channels?',
                                '4.How many comments were made on each video, and what are their corresponding video names?',
                                '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6.What is the total number of likes for each video, and what are their corresponding video names?',
                                '7.What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8.What are the names of all the channels that have published videos in the year 2022?',
                                '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10.Which videos have the highest number of comments, and what are their corresponding channel names?'),index=None)
    
        
if question == '1.What are the names of all the videos and their corresponding channels?':
    query1="select video_name AS Video_Title, A.channel_name AS Channel_Name FROM video_details AS A INNER JOIN channels_data AS B ON A.channel_id=B.channel_id"
    mycursor.execute(query1)
    q1=mycursor.fetchall()
    df=pd.DataFrame(q1,columns=mycursor.column_names)
    st.write(df)

elif question == '2.Which channels have the most number of videos, and how many videos do they have?':
    query2="select channel_name,Total_videos as NO_OF_Videos from Channels_data ORDER BY Total_videos DESC"
    mycursor.execute(query2)
    q2=mycursor.fetchall()
    df=pd.DataFrame(q2,columns=mycursor.column_names)
    st.write(df)

elif question == '3.What are the top 10 most viewed videos and their respective channels?':
    query3 = "select B.channel_name AS Channel_Name, video_name AS Video_Title ,A.Views AS Views from video_details as a inner join channels_data as b on a.channel_id=b.channel_id order by Views desc limit 10"
    mycursor.execute(query3)
    q3=mycursor.fetchall()
    df=pd.DataFrame(q3,columns=mycursor.column_names)
    st.write(df)
    
elif question == '4.How many comments were made on each video, and what are their corresponding video names?':
    query4= "select video_name AS Video_Title,Comment_Count from video_details order by Comment_Count desc"
    mycursor.execute(query4)
    q4=mycursor.fetchall()
    df=pd.DataFrame(q4,columns=mycursor.column_names)
    st.write(df)
    
elif question == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = "select B.channel_name AS Channel_Name, video_name AS Video_Title, A.Likes AS Likes from video_details as a inner join channels_data as b on a.channel_id=b.channel_id order by Likes desc limit 10"
    mycursor.execute(query5)
    q5=mycursor.fetchall()
    df=pd.DataFrame(q5,columns=mycursor.column_names)
    st.write(df)
    
elif question == '6.What is the total number of likes for each video, and what are their corresponding video names?':
    query6= "select video_name AS Video_Title, Likes from video_details order by Likes desc"
    mycursor.execute(query6)
    q6=mycursor.fetchall()
    df=pd.DataFrame(q6,columns=mycursor.column_names)
    st.write(df)

elif question == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query7= "select Channel_name,Views from channels_data"
    mycursor.execute(query7)
    q7=mycursor.fetchall()
    df=pd.DataFrame(q7,columns=mycursor.column_names)
    st.write(df)
    
elif question == '8.What are the names of all the channels that have published videos in the year 2022?':
    query8= "select A.video_name AS Video_Title,A.Published_date as ReleaseDate,B.channel_name AS Channel_Name from video_details as a inner join channels_data as b on a.channel_id=b.channel_id where Published_date  BETWEEN '2022-01-01' AND '2022-12-31' "
    mycursor.execute(query8)
    q8=mycursor.fetchall()
    df=pd.DataFrame(q8,columns=mycursor.column_names)
    st.write(df)
    
elif question == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9= "select B.channel_name AS Channel_Name, avg(A.Duration) as Avgduration from video_details as a inner join channels_data as b on a.channel_id=b.channel_id group by  Channel_Name"
    mycursor.execute(query9)
    q9=mycursor.fetchall()
    df=pd.DataFrame(q9,columns=mycursor.column_names)
    st.write(df)
    
elif question == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10= "select B.channel_name AS Channel_Name,A.video_name AS Video_Title,A.Comment_Count as TotalComments from video_details as A inner join channels_data as B on a.channel_id=b.channel_id order by A.Comment_Count desc limit 10"
    mycursor.execute(query10)
    q10=mycursor.fetchall()
    df=pd.DataFrame(q10,columns=mycursor.column_names)
    st.write(df)

