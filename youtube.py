#YOUTUBE DATA HARVESTING AND WAREHOUSING

import googleapiclient.discovery
import pandas as pd
import streamlit as st
import mysql.connector
import pandas as pd
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError


#API_Key_and_Connect_Youtube_Server

api_key ='AIzaSyCtC5qGu9d4qfsK7cC9iJgpkGJXpjeM3Lw'
api_service_name ="youtube"
api_version ="v3"

youtube = googleapiclient.discovery.build(api_service_name,api_version, developerKey=api_key)

#MYSQL_Connection:

mydb = mysql.connector.connect(host="localhost",user="root",password="")

mycursor = mydb.cursor(buffered=True)
db_connection_str = f"mysql+mysqlconnector://root@localhost/Project_1"
db_engine = create_engine(db_connection_str)
#mycursor.execute('create database  Project_1')
mycursor.execute('use Project_1')

#Display data in the Streamlit app

st.set_page_config(layout='wide')
st.subheader(":red[YouTube Data] :blue[Harvesting] and :blue[Warehousing]...📡",divider='rainbow')
st.markdown(" ")
st.markdown(" ")

col1,col2= st.columns(2)
with col1:
    st.subheader(':blue[Description]', divider='grey')
    st.write("YouTube is an American online, free video sharing social media website and app on the internet. and founded on February 14, 2005, by three former PayPal employees. Google (a search engine company) has owned and operated YouTube since 2006")
    st.link_button("Youtube Link", "https://www.youtube.com/")
    st.markdown(":blue[Domain :] Social Media")
    st.markdown(":blue[Technologies used :] Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL ")
    

with col2:
    st.image(r"C:\Users\TOSHIBA\Downloads\YouTube-White.svg",width= 400)
    st.markdown(" ")

channel_id = st.text_input("Enter Your Channel ID :")


def channel_data_df(channel_id):

    processed_channel_ids = set()

    if channel_id in processed_channel_ids:
        st.error(f"Channel ID {channel_id} has already been processed.")
        return None
    
    try:
        try:

            request = youtube.channels().list(
            part="contentDetails,snippet,statistics",
            id= channel_id)
            response = request.execute()

            if 'items' not in response:
                st.error(f"Invalid Channel id: {channel_id}")
                st.error("Enter the Valid Channel id")
                return None
                
        except HttpError as e:
            st.error('Something Wrong, Check your Internet Connection & Try again!', icon='🚨')
            st.error('An error occurred: %s' % e)
            return None
    except:
            st.error('Limit Reached, Try again later!.')


    data={ 'channel_id':channel_id,
            'channel_name':response['items'][0]['snippet']['title'], 
            'channel_des':response['items'][0]['snippet']['description'],
            'channel_playid':response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            'channel_vidcount':response['items'][0]['statistics']['videoCount'],
            'channel_viewcount':response['items'][0]['statistics']['viewCount'],
            'channel_subcount':response['items'][0]['statistics']['subscriberCount']}
            
    processed_channel_ids.add(channel_id)

    return data


def all_video_Ids(channel_id):

    all_video_ids=[]

    
    video_ids=[]
    request1=youtube.channels().list(part="contentDetails",id= channel_id)
    response=request1.execute()

    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
        
    while True:
        request2 = youtube.playlistItems().list(
                    part="snippet",
                    maxResults=50,
                    pageToken=next_page_token,
                    playlistId=playlist_id)
        response = request2.execute()

        for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response.get('nextPageToken')

        if next_page_token is None:
                break
                
    return all_video_ids

def video_details_info_df(Playlist_Information):

    video_data=[]

    for video_id in Playlist_Information:
        request3 = youtube.videos().list(
                part="contentDetails,snippet,statistics",
                id=video_id)
        response2 = request3.execute()

        for details in response2["items"]:
            data= {'Video_Id':details['id'],
                'Video_title':details['snippet']['title'],
                'channel_id':details['snippet']['channelId'],
                'Video_Description':details['snippet']['description'],
                'Video_pubdate':details['snippet']['publishedAt'],
                'Video_thumbnails':details['snippet']['thumbnails']['default']['url'],
                'Video_viewcount':details['statistics']['viewCount'],
                'Video_likecount':details['statistics'].get('likeCount', 0),
                'Video_favoritecount':details['statistics']['favoriteCount'],
                'Video_commentcount':details['statistics'].get('commentCount', 0),
                'Video_duration':(details['contentDetails']['duration']),
                'Video_caption':details['contentDetails']['caption']
            }

            video_data.append(data)

    
        
    return video_data



def comment_details_info_df(Playlist_Information):
    comment_data = []
    
    try:
        for a in Playlist_Information:
            request4 = youtube.commentThreads().list(
                        part="snippet",
                        videoId=a,
                        maxResults=10)
            response3 = request4.execute()

            for comment in response3["items"]:
                        data = {
                            'comment_id': comment['snippet']['topLevelComment']['id'],
                            'video_id': comment['snippet']['topLevelComment']['snippet']['videoId'],
                            'channel_id': comment['snippet']['channelId'],
                            'author_name': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'text_display': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'published_date': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
                        comment_data.append(data) 
    except:
        pass
    
    return comment_data




# Streamlit Code

get_data=st.button(':red[Proceed to Manage Data from the above Channel Id]')

if "Get_state" not in st.session_state:
    st.session_state.Get_state = False

if get_data or st.session_state.Get_state:
    st.session_state.Get_state = True

if get_data:

        Channel_information=channel_data_df(channel_id)
        Playlist_Information=all_video_Ids(channel_id)
        Video_information= video_details_info_df(Playlist_Information)
        Comment_information=comment_details_info_df(Playlist_Information)

        Channel_details=pd.DataFrame([Channel_information])
        Video_details=pd.DataFrame(Video_information)
        Comment_details=pd.DataFrame(Comment_information)
        st.write(":green[Data has been collected and Stored Successfully!]")

# Migrate Data:

if st.button(':red[Migrate Data to MySQL]'):
    
    #MYSQL_Localhost_Connection:
    try:
        mydb = mysql.connector.connect(host="localhost",user="root",password="")
        mycursor = mydb.cursor(buffered=True)

        db_connection_str = f"mysql+mysqlconnector://root@localhost/Project_1"
        db_engine = create_engine(db_connection_str)
        mycursor.execute('create database if not exists Project_1')
        mycursor.execute('use Project_1')
    
        Channel_information=channel_data_df(channel_id)
        Playlist_Information=all_video_Ids(channel_id)
        Video_information= video_details_info_df(Playlist_Information)
        Comment_information=comment_details_info_df(Playlist_Information)

        Channel_details=pd.DataFrame([Channel_information])
        Video_details=pd.DataFrame(Video_information)
        Comment_details=pd.DataFrame(Comment_information)

#MYSQL_Channels_Table_Creation:

        mycursor.execute('create database if not exists  Project_1')
        mycursor.execute('use Project_1')
        mycursor.execute('''create table if not exists channels(channel_id VARCHAR(50)PRIMARY KEY , 
                    channel_name VARCHAR(100),
                    channel_des TEXT,
                    channel_playid VARCHAR(40), 
                    channel_vidcount INT(5), 
                    channel_viewcount INT(10), 
                    channel_subcount INT(10))''')
    
        mydb.commit()
        Channel_details.to_sql('channels', con=db_engine, if_exists='append', index=False)


#MYSQL_Videos_Table_Creation:

        mycursor.execute('use Project_1')
        mycursor.execute('''create table if not exists videos (channel_id VARCHAR(50),FOREIGN KEY(channel_id) references channels(channel_id),
                        Video_Id VARCHAR(50),
                        Video_title TEXT,
                        Video_Description TEXT,
                        Video_pubdate VARCHAR(30),
                        Video_thumbnails TEXT,
                        Video_viewcount INT(15),
                        Video_likecount INT(15),
                        Video_favoritecount INT(15),
                        Video_commentcount INT(15), 
                        Video_duration VARCHAR(10), 
                        Video_caption VARCHAR(10)) ''')

        mydb.commit()
        Video_details.to_sql(name='videos', con=db_engine, if_exists='append', index=False)


#MYSQL_Comments_Table_Creation:

        mycursor.execute('use Project_1')
        mycursor.execute('''create table if not exists comments (comment_id VARCHAR(30),channel_id VARCHAR(50),
                        FOREIGN KEY(channel_id) references channels(channel_id),
                        video_id VARCHAR(15),                        
                        author_name LONGTEXT,
                        text_display TEXT,
                        published_date VARCHAR(20)) ''')

        mydb.commit()

        Comment_details.to_sql(name='comments', con=db_engine, if_exists='append', index=False)
        st.success(":green[Data Migration Complete & Ready to Proceed!]")
    
    except IntegrityError as e:
            st.error(f"This Channel id is already exists")
            
    
    
tab1,tab2,tab3,tab4=st.tabs(["Channel Information","Video Information","Comment Information","Select Quries"])


with tab1:
    st.header(":blue[Channel Information]")
    mycursor.execute("SELECT Channel_name,Channel_id,channel_Viewcount,channel_Subcount,channel_Vidcount from Project_1.Channels")

    Channel_details=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(Channel_details)

with tab2: 
    st.header(":blue[Video Information]")
    mycursor.execute("SELECT Video_Title,Video_Id,Video_Pubdate,Video_duration,Video_viewcount,Video_likecount from Project_1.Videos")

    Video_df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(Video_df)

with tab3:
    st.header(":blue[Comment Information]")
    mycursor.execute("SELECT * from Project_1.comments")

    Comment_df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(Comment_df)


#QUERIES_are_Visible_in_the_Streamlit

with tab4:
    query_select=st.selectbox(":blue[Ten Queries :]",("Select your Query",
    "1.What are the names of all the videos and their corresponding channels?",
    "2.Which channels have the most number of videos, and how many videos do they have?",
    "3.What are the top 10 most viewed videos and their respective channels?",
    "4.How many comments were made on each video, and what are their corresponding video names?",
    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
    "8.What are the names of all the channels that have published videos in the year 2022?",
    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

#MYSQL_Connection

mydb = mysql.connector.connect(host="localhost",user="root",password="")

mycursor = mydb.cursor(buffered=True)
mycursor.execute('USE Project_1')

#Query the SQL data warehouse: 

if query_select=="1.What are the names of all the videos and their corresponding channels?":
    mycursor.execute("SELECT videos.Video_title, channels.channel_name \
    FROM Project_1.videos \
    INNER JOIN channels ON videos.channel_id = channels.channel_id")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[name of videos and corresponding channels]")
    st.write(df)

elif query_select=="2.Which channels have the most number of videos, and how many videos do they have?":
    mycursor.execute('SELECT channel_name,max(channel_vidcount) as max_videocount FROM Project_1.channels LIMIT 1')

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":blue[Maximum video count]")
    st.write(df)

elif query_select== "3.What are the top 10 most viewed videos and their respective channels?":
    mycursor.execute("SELECT channels.channel_name, videos.Video_viewcount \
                    FROM Project_1.videos \
                    JOIN channels ON videos.channel_id = channels.channel_id \
                    ORDER BY videos.Video_viewcount DESC LIMIT 10")
    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[Top 10 Most viewed video and their channel name]")
    st.write(df)

elif query_select=="4.How many comments were made on each video, and what are their corresponding video names?":
    mycursor.execute("SELECT Video_title,Video_commentcount FROM Project_1.videos ORDER BY Video_commentcount DESC")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[comments on each video and cor video name]")
    st.write(df)

elif query_select=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    mycursor.execute("SELECT channels.channel_name, videos.Video_likecount \
    FROM Project_1.videos \
    JOIN channels ON videos.channel_id = channels.channel_id \
    WHERE videos.Video_likecount = (SELECT MAX(Video_likecount) FROM videos)")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[Highest Likes and their channel name]")
    st.write(df)

elif query_select=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    mycursor.execute("SELECT Video_title,Video_likecount FROM Project_1.videos GROUP BY Video_title ORDER BY Video_likecount DESC")
    st.write(":green[Total number of likes and their channel names]")
    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)

elif query_select== "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    mycursor.execute('SELECT channel_name,channel_viewcount  FROM Project_1.channels ORDER by channel_viewcount  DESC')
    
    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[views for each channel]")
    st.write(df)

elif query_select=="8.What are the names of all the channels that have published videos in the year 2022?":
    mycursor.execute("SELECT DISTINCT channels.channel_name \
                    FROM Project_1.channels \
                    JOIN videos ON channels.channel_id = videos.channel_id \
                    WHERE YEAR(Video_pubdate) = 2022")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[name of channels published video on 2022]")
    st.write(df)

elif query_select== "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    mycursor.execute("SELECT channels.channel_name, SEC_TO_TIME(AVG(videos.Video_duration)) AS average_duration \
                    FROM Project_1.videos \
                    JOIN channels ON videos.channel_id = channels.channel_id \
                    GROUP BY channels.channel_name")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[Average video duration for each channels]")
    st.write(df)


elif query_select== "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    mycursor.execute("SELECT channels.channel_name, videos.Video_title, videos.Video_commentcount \
                    FROM Project_1.videos \
                    JOIN channels ON videos.channel_id = channels.channel_id \
                    ORDER by (Video_commentcount) DESC LIMIT 10")

    df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(":green[Highest number of comments and their channel name]")
    st.write(df)
        