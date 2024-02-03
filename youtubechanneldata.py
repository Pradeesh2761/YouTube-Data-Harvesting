#packinges
from googleapiclient.discovery import build
import pandas as pd
import pymongo
import psycopg2
import streamlit as st

#API connection
def Api_conn():

    api_service_name = "youtube"
    api_version = "v3"
    api_key = "AIzaSyCzjGhFKk7Hj4PiPo4KQU2n2uKkae2AoNQ"
    youtube = build(api_service_name,api_version,developerKey=api_key)
    return youtube

youtube = Api_conn()



#get channel info:
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id = channel_id
        )
    response = request.execute()

    for i in response["items"]:
        data = dict(Channel_id = i["id"],
                    Channel_name = i["snippet"]["title"], 
                    Total_Subscriber = i["statistics"]["subscriberCount"],
                    Total_views = i["statistics"]["viewCount"],
                    Total_videos = i["statistics"]["videoCount"],
                    Channel_description = i['snippet']['description'],
                    Playlist_id = i["contentDetails"]["relatedPlaylists"]["uploads"],
                    Channel_Started_date = i["snippet"]["publishedAt"])
    return data   
   
            
            
#get video id's using playlist id:
def Video_ID(Channel_id):
    video_ids = []
    response = youtube.channels().list(id = Channel_id,
                                    part = "contentDetails").execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists'][ 'uploads']
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                                part = "snippet",
                                                playlistId = playlist_id,
                                                maxResults = 50,
                                                pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return  video_ids



#get videos info :
def get_video_info(video_ids):
    video_data = []
    for Video_id in video_ids:
        request = youtube.videos().list(
                                        part="snippet,contentDetails,statistics",
                                        id = Video_id )
        response = request.execute()

        for item in response['items']:
            data = dict(channel_name = item['snippet'][ 'channelTitle'],
                        channel_id = item['snippet']['channelId'],
                        video_id = item['id'],
                        title = item['snippet']['title'],                   
                        thumbnail = item['snippet']['thumbnails']['default']['url'],
                        description = item["snippet"].get('description'),
                        published_date = item['snippet']['publishedAt'],
                        duration = item['contentDetails']['duration'],
                        view = item['statistics'].get('viewCount'),
                        likes = item['statistics'].get('likeCount'),
                        comment_count = item['statistics'].get('commentCount'),
                        favorite_count = item['statistics']['favoriteCount'],
                        definition = item['contentDetails']['definition'],
                        caption_status = item['contentDetails']['caption']
                        )
            video_data.append(data)          
    return video_data
  

#get comment info :
def get_comment_info(video_id):
    comment_data =[]
    try:
        for Video_ID in video_id:
            request = youtube.commentThreads().list(
                                                    part = 'snippet',
                                                    videoId = Video_ID,
                                                    maxResults = 50,
                                                    )
            response = request.execute()

            for item in response['items']:
                data = dict(comment_id = item['snippet']['topLevelComment']['id'],
                            videoid = item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published_date = item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                comment_data.append(data)
    except:
        pass 
    return comment_data         


#get_playlist_details:
def get_playlist_details(Channel_id):
        
    next_page_token = None
    All_data = []
    while True:
            request = youtube.playlists().list(
                                            part = 'snippet,contentDetails',
                                            channelId = Channel_id,
                                            maxResults = 50,
                                            pageToken = next_page_token
                                            )
            response = request.execute()

            for item in response['items']:
                    data = dict(playlist_id = item['id'],
                                    Title = item['snippet']['title'],
                                    Channel_id = item['snippet']['channelId'],
                                    Channel_name = item['snippet']['channelTitle'],
                                    Piblished_date = item['snippet']['publishedAt'],
                                    video_count = item['contentDetails']['itemCount']
                                    )
                    All_data.append(data)
            break
    return All_data



#creat database on mongodb in py :
client = pymongo.MongoClient("mongodb+srv://Pradeesh:adminpradeesh@cluster0.c6gotg6.mongodb.net/?retryWrites=true&w=majority")
db = client["youtube_data"]

#upload in mongodb :
def channels_detail(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = Video_ID(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids) 

    coll1 = db["channel_details"]   
    coll1.insert_one({"channel_information":ch_details, 
                        "playlist_information":pl_details,
                        "video_information":vi_details,
                        "comment_information":com_details})
    return "uploaded completed"


# connect to postgre( and table creation for channel_details,playlist,videos,comments)
# creating channel table : 

def channels_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "1234",                       
                            database = "utube_data",
                            port = "5432" )
    cursor = mydb.cursor()

    drop = '''drop table if exists channels_detail'''
    cursor.execute(drop)
    mydb.commit()
    try:
        create_query = '''Create table if not exists channels_detail(Channel_name varchar(100),
                                                                    Channel_id varchar(80) primary key,
                                                                    Total_Subscriber bigint,
                                                                    Total_views bigint,
                                                                    Total_videos int,
                                                                    Channel_description text,
                                                                    Playlist_id varchar(50))'''
        cursor.execute(create_query)  
        mydb.commit()    
    except:
        print("channels table already created") 
        
# insert values into channel_details table
        
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append((ch_data["channel_information"]))
    df = pd.DataFrame(ch_list) 

    for index,row in df.iterrows():
        insert_query = ''' insert into channels_detail(Channel_id,
                                                    Channel_name,
                                                    Total_Subscriber,
                                                    Total_views,
                                                    Total_videos,
                                                    Channel_description,
                                                    Playlist_id)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row["Channel_id"],
                row["Channel_name"],
                row["Total_Subscriber"],
                row["Total_views"],
                row["Total_videos"],
                row["Channel_description"],
                row["Playlist_id"])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("Channels values are already inserted")    


# creating playlist table

def playlist_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "1234",                       
                            database = "utube_data",
                            port = "5432" )
    cursor = mydb.cursor()

    drop = '''drop table if exists channels_playlist'''
    cursor.execute(drop)
    mydb.commit()

    create_query = '''Create table if not exists channels_playlist(playlist_id varchar(100) primary key,
                                                                Title varchar(80),
                                                                Channel_id varchar(80),
                                                                Channel_name varchar(80),
                                                                Piblished_date timestamp,
                                                                video_count int
                                                                )'''
    cursor.execute(create_query)  
    mydb.commit()    

# insert values into playlist table
   
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1 = pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query = ''' insert into channels_playlist(playlist_id,
                                                    Title,
                                                    Channel_id,
                                                    Channel_name,
                                                    Piblished_date,
                                                    video_count
                                                    )

                                                    values(%s,%s,%s,%s,%s,%s)'''

        values = (row["playlist_id"],
                row["Title"],
                row["Channel_id"],
                row["Channel_name"],
                row["Piblished_date"],
                row["video_count"])              
      
        cursor.execute(insert_query,values)
        mydb.commit()
    
        print("playlist values are already inserted")     


# creating video table

def video_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "1234",                       
                            database = "utube_data",
                            port = "5432" )
    cursor = mydb.cursor()

    drop = '''drop table if exists channels_video_info'''
    cursor.execute(drop)
    mydb.commit()

    create_query = '''Create table if not exists channels_video_info(channel_name varchar(255),
                                                            channel_id varchar(255),
                                                            video_id varchar(30) primary key,
                                                            title varchar(255),
                                                            thumbnail varchar(255),
                                                            description text,
                                                            published_date timestamp,
                                                            duration interval,
                                                            view bigint,
                                                            likes bigint,
                                                            comment_count int,
                                                            favorite_count int,
                                                            definition varchar(255),
                                                            caption_status varchar(255)
                                                                )'''
    cursor.execute(create_query)  
    mydb.commit()    

# insert values into video table   

    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2 = pd.DataFrame(vi_list)


    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "1234",                       
                            database = "utube_data",
                            port = "5432" )
    cursor = mydb.cursor()

    for index,row in df2.iterrows():
        insert_query = ''' insert into channels_video_info(channel_name,
                                                    channel_id,
                                                    video_id,
                                                    title,
                                                    thumbnail,
                                                    description,
                                                    published_date,
                                                    duration,
                                                    view,
                                                    likes,
                                                    comment_count,
                                                    favorite_count,
                                                    definition,
                                                    caption_status)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['channel_name'],
                row['channel_id'],
                row['video_id'],  
                row['title'],
                row['thumbnail'],
                row['description'],
                row['published_date'],
                row['duration'],
                row['view'],
                row["likes"],
                row['comment_count'],
                row['favorite_count'],
                row['definition'],
                row['caption_status'])              
    
        cursor.execute(insert_query,values)
        mydb.commit()


# creating comment table :

def comment_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "1234",                       
                            database = "utube_data",
                            port = "5432" )
    cursor = mydb.cursor()

    drop = '''drop table if exists channels_comment'''
    cursor.execute(drop)
    mydb.commit()

    create_query = '''Create table if not exists channels_comment(comment_id varchar(100) primary key,
                                                                videoid varchar(80),
                                                                comment_text text,
                                                                comment_author varchar(255),
                                                                comment_published_date timestamp
                                                                )'''
    cursor.execute(create_query)  
    mydb.commit() 

    # insert values in comment table :

    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3 = pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query = ''' insert into channels_comment(comment_id,
                                                    videoid,
                                                    comment_text,
                                                    comment_author,
                                                    comment_published_date
                                                    )

                                                    values(%s,%s,%s,%s,%s)'''

        values = (row["comment_id"],
                row["videoid"],
                row["comment_text"],
                row["comment_author"],
                row["comment_published_date"])
                        

        cursor.execute(insert_query,values)
        mydb.commit()        


def tables():
    channels_table()
    playlist_table()    
    video_table()
    comment_table()  

    return "tables created sucessfully"          

def show_channels_table():
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append((ch_data["channel_information"]))
    df = st.dataframe(ch_list)
    return df

def show_playlist_table():
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1 = st.dataframe(pl_list)
    return df1

def show_videos_table():
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2 = st.dataframe(vi_list)
    return df2

def show_comments_table():
    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3 = st.dataframe(com_list)
    return df3

# streamlite part :

with st.sidebar:
     st.title(":red[YOUTUBE] DATA HARVESTING AND WAREHOUSING")
     st.header("skill Take Away", divider='red')
     st.caption("Python Scripting")
     st.caption("Data Collection")
     st.caption("MongoDB")
     st.caption("API integration")
     st.caption("Data Management using MongoDB (Atlas) and SQL")


channel_ids = st.text_input("Enter the channel ID ")

if st.button("Collect the Data"):
    ch_ids = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])

    if channel_ids in ch_ids:
        st.success("Given Channel id was already exist")
    else:
        insert = channels_detail(channel_ids)
        st.success(insert)

if st.button("Migrate to sql"):
    Table = tables()
    st.success(Table)

show_table = st.radio("Select the Table for view",("Channels","Playlist","Videos","Comments"))  

if show_table == "Channels":
    show_channels_table()
elif show_table == "Playlist":
    show_playlist_table()
elif show_table == "Videos":
    show_videos_table()
elif show_table == "Comments":
    show_comments_table()

#sql connection :

mydb = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        password = "1234",                       
                        database = "utube_data",
                        port = "5432" )
cursor = mydb.cursor()

Quest = st.selectbox("Select Your Question",("1. What are the names of all the videos and their corresponding channels?",
                                             "2. Which channels have the most number of videos, and how many videos do they have?",
                                             "3. What are the top 10 most viewed videos and their respective channels?", 
                                             "4. How many comments were made on each video, and what are their corresponding video names?",
                                             "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                             "6. What is the total number of likes for each video, and what are  their corresponding video names?",
                                             "7. What is the total number of views for each channel, and what are their  corresponding channel names?",
                                             "8. What are the names of all the channels that have published videos in the year 2022?",
                                             "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                             "10. Which videos have the highest number of comments, and what are their corresponding channel names?"    
                                                   ))    
#1
if Quest == "1. What are the names of all the videos and their corresponding channels?":

    query1 = '''select title as videos,channel_name as channelname from channels_video_info'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall() 
    dataf = pd.DataFrame(t1,columns = ["video title","channel name"])
    st.write(dataf)
#2
elif Quest == "2. Which channels have the most number of videos, and how many videos do they have?":

    query2 = '''select channel_name as channelname, total_videos as no_of_count from channels_detail
                 order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall() 
    dataf2 = pd.DataFrame(t2,columns = ["channel name","no.of videos"])
    dataf2
    st.write(dataf2)
#3
elif Quest == "3. What are the top 10 most viewed videos and their respective channels?":

    query3 = '''select view as views, channel_name as channelname, title as video_title from channels_video_info
                where view is not null order by view desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall() 
    dataf3 = pd.DataFrame(t3,columns = ["no.of views","channel name", "video title"])
    st.write(dataf3)
#4
elif Quest == "4. How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select comment_count as no_of_comments, title as video_title from channels_video_info 
                    where comment_count is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall() 
    dataf4 = pd.DataFrame(t4,columns = ["no.of comment", "video title"])
    st.write(dataf4)
#5
elif Quest == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select title as video_title,likes as no_of_likes, channel_name as channelname  from channels_video_info 
                    where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall() 
    dataf5 = pd.DataFrame(t5,columns = ["video title", "no.of likes", "channel name"])
    
    st.write(dataf5)
#6
elif Quest == "6. What is the total number of likes for each video, and what are  their corresponding video names?":
    query6 = '''select title as video_title,likes as no_of_likes from channels_video_info 
                where likes is not null order by likes desc'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall() 
    dataf6 = pd.DataFrame(t6,columns = ["video title", "no.of likes"])
    st.write(dataf6)
#7 
elif Quest == "7. What is the total number of views for each channel, and what are their  corresponding channel names?":
    query7 = '''select channel_name as channelname,total_views as no_of_views from channels_detail'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall() 
    dataf7 = pd.DataFrame(t7,columns = ["channels name", "no of views"])
    st.write(dataf7)
#8
elif Quest == "8. What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select channel_name as channelname,published_date as publisheddate from channels_video_info
                where extract(year from published_date) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall() 
    dataf8 = pd.DataFrame(t8,columns = ["channels name", "video published at year 2022"])
    st.write(dataf8)
#9
elif Quest == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from channels_video_info
            group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall() 
    dataf9 = pd.DataFrame(t9,columns = ["channels name", "AverageDuration"])
    
    t9=[]
    for index,row in dataf9.iterrows():
        channel_title = row["channels name"]
        averageduration = row["AverageDuration"]
        average_duration_str = str(averageduration)
        t9.append(dict(channelname = channel_title,AverageDuration = average_duration_str))
    dataframe = pd.DataFrame(t9)
    st.write(dataframe)
    #10
elif Quest == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select channel_name as channelname,title as video_title ,comment_count as totalcomment from channels_video_info
              where comment_count is not null order by comment_count desc'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall() 
    dataf10 = pd.DataFrame(t10,columns = ["channels name", "video title", "No.of comments"])
    st.write(dataf10)


    #streamlit run youtubechanneldata.py
