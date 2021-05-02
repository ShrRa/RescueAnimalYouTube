from apiclient.discovery import build 
import os
import pandas as pd

with open('apiKey.ini') as f:
    apiKey=f.readline()

# Arguments that need to passed to the build function 
DEVELOPER_KEY = apiKey
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = 'v3'

# creating Youtube Resource Object 
youtube_object = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, 
                                        developerKey = DEVELOPER_KEY) 

def getNumOfVids(query, publishedAfter,publishedBefore):
    search_keyword = youtube_object.search().list(q = query, part = "id", type='video',
                                                 publishedAfter=publishedAfter,
                                                  publishedBefore=publishedBefore).execute()
    print('Number of videos: ', search_keyword['pageInfo']['totalResults'])
    return

def searchVids(query, publishedAfter,publishedBefore,maxResults=50):
    videos = pd.DataFrame(columns=['videoID','publishedAt',
                                  'title','description','thumbnailUrl',
                                  'channelId','channelTitle'])
    nextPage=True
    pageToken=None
    
    while nextPage==True:
        search_keyword = youtube_object.search().list(q = query, part = "id, snippet", type='video',
                                               maxResults = maxResults,pageToken=pageToken,
                                                 publishedAfter=publishedAfter,
                                                     publishedBefore=publishedBefore).execute()
        print(search_keyword['pageInfo']['resultsPerPage'],'/',search_keyword['pageInfo']['totalResults'])
        results = search_keyword.get("items", []) 
        # extracting required info from each result object 
        for result in results: 
            video={}
            video['videoID']=result['id']['videoId']
            video['publishedAt']=result['snippet']['publishedAt']
            video['title']=result['snippet']['title']
            video['description']=result['snippet']['description']
            video['thumbnailUrl']=result['snippet']['thumbnails']['default']['url']
            video['channelId']=result['snippet']['channelId']
            video['channelTitle']=result['snippet']['channelTitle']
            videos=videos.append(video,ignore_index=True)
        if len(videos)<maxResults and 'nextPageToken' in search_keyword.keys():
            pageToken=search_keyword['nextPageToken']
        else:
            nextPage=False

    return videos

def getVidInfo(vidId):
    video_response=youtube_object.videos().list(
                    part="snippet,contentDetails,statistics",id=vidId).execute()
    vidInfo=pd.Series(index=['videoID','tags','duration','viewCount',
            'likeCount','dislikeCount','commentCount'])
    vidInfo['videoID']=vidId
    vidResponseDict=video_response['items'][0]
    if vidResponseDict!=[]:
        try:
            vidInfo['tags']=', '.join(video_response['items'][0]['snippet']['tags'])
        except:
            pass
        try:
            vidInfo['duration']=video_response['items'][0]['contentDetails']['duration']
        except:
            pass
        try:
            vidInfo['viewCount']=video_response['items'][0]['statistics']['viewCount']
        except:
            pass
        try:
            vidInfo['likeCount']=video_response['items'][0]['statistics']['likeCount']
        except:
            pass
        try:
            vidInfo['dislikeCount']=video_response['items'][0]['statistics']['dislikeCount']
        except:
            pass
        try:
            vidInfo['commentCount']=video_response['items'][0]['statistics']['commentCount']
        except:
            pass
    else:
        print(vidID,' has mysteriously disappeared')
    return vidInfo

def getVidsStats(videos):
    vidStats=pd.DataFrame(columns=['videoID','tags','duration','viewCount','likeCount',
                                  'dislikeCount','commentCount'])
    totCount=len(videos)
    print('Retrieving info about ', totCount,' videos')
    
    videos[['videoID','tags','duration','viewCount','likeCount',
                                  'dislikeCount','commentCount']]=videos['videoID'].apply(getVidInfo)
    videos['viewCount']=videos['viewCount'].astype(float)
    videos['likeCount']=videos['likeCount'].astype(float)
    videos['dislikeCount']=videos['dislikeCount'].astype(float)
    videos['commentCount']=videos['commentCount'].astype(float)
    return videos

def getComments(videoId):
    commentsDF = pd.DataFrame(columns=['videoId','publishedAt',
                                  'textOriginal','likeCount'])
    nextPage=True
    pageToken=None
    print('Retrieving comments')
    while nextPage==True:
        comments=youtube_object.commentThreads().list(
                    part="snippet,replies",pageToken=pageToken,videoId=videoId,maxResults=100).execute()
        print(comments['pageInfo']['totalResults'])
        
        for item in comments['items']:
            comment={'videoId':videoId}
            comment['textOriginal']=item['snippet']['topLevelComment']['snippet']['textOriginal']
            comment['publishedAt']=item['snippet']['topLevelComment']['snippet']['publishedAt']
            comment['likeCount']=item['snippet']['topLevelComment']['snippet']['likeCount']
            commentsDF=commentsDF.append(comment,ignore_index=True)
        if 'nextPageToken' in comments.keys():
            pageToken=comments['nextPageToken']
        else:
            nextPage=False
    commentsDF['likeCount']=commentsDF['likeCount'].astype(float)
    return commentsDF

def getChannels(channels):
    channelInfo=pd.DataFrame(columns=['channelId','title','description','keywords',
                                     'viewCount','subscriberCount','hiddenSubscriberCount','videoCount',
                                     'country','publishedAt','uploadsPlaylistId'])
    channelInfo['channelId']=channels['channelId']
    totCount=len(channels)
    print('Retrieving info about ',totCount,' channels')
    
    channelInfo[['channelId','title','description','keywords',
                 'viewCount','subscriberCount','hiddenSubscriberCount','videoCount',
                 'country','publishedAt',
                 'uploadsPlaylistId']]=channelInfo['channelId'].apply(getChannelInfo)
    channelInfo['viewCount']=channelInfo['viewCount'].astype(float)
    channelInfo['subscriberCount']=channelInfo['subscriberCount'].astype(float)
    channelInfo['videoCount']=channelInfo['videoCount'].astype(float)
    return channelInfo

def getChannelInfo(channelId):
    channels=youtube_object.channels().list(part='brandingSettings,contentDetails,snippet,statistics',
                                        id=channelId).execute()
    
    channelInfoDict=pd.Series(index=['channelId','title','description','keywords',
                                     'viewCount','subscriberCount',
                                     'hiddenSubscriberCount','videoCount',
                                     'country','publishedAt','uploadsPlaylistId'])
    channelInfoDict['channelId']=channelId
    channel=channels['items'][0]
    if channel!=[]:
        try:
            channelInfoDict['viewCount']=channel['statistics']['viewCount']
        except:
            pass
        try:
            channelInfoDict['subscriberCount']=channel['statistics']['subscriberCount']
        except:
            pass
        try:
            channelInfoDict['hiddenSubscriberCount']=channel['statistics']['hiddenSubscriberCount']
        except:
            pass
        try:
            channelInfoDict['videoCount']=channel['statistics']['videoCount']
        except:
            pass
        try:
            channelInfoDict['title']=channel['snippet']['title']
        except:
            pass
        try:
            channelInfoDict['keywords']=channel['brandingSettings']['channel']['keywords']
        except:
            pass
        try:
            channelInfoDict['description']=channel['snippet']['description']
        except:
            pass
        try:
            channelInfoDict['country']=channel['snippet']['country']
        except:
            pass
        try:
            channelInfoDict['publishedAt']=channel['snippet']['publishedAt']
        except:
            pass
        try:
            channelInfoDict['uploadsPlaylistId']=channel['contentDetails']['relatedPlaylists']['uploads']
        except:
            pass
    else:
        print(channelId,' has mysteriously disappeared')
    return channelInfoDict

def getVidsFromChannel(playlistID,verbosityStep=10):
    vids = pd.DataFrame(columns=['videoID','title','description','thumbnail','publishedAt','privacyStatus'])
    nextPage=True
    pageToken=None
    i=0
    print('Retrieving info about the videos from a channel')
    while nextPage==True:
        vidsFromChannel=youtube_object.playlistItems().list(part='snippet,status',
                                        playlistId=playlistID,maxResults=50,pageToken=pageToken).execute()
        print(i,': ',vidsFromChannel['pageInfo']['resultsPerPage'],'/',vidsFromChannel['pageInfo']['totalResults'])
        i+=1
        for item in vidsFromChannel['items']:
            vid={'videoID':item['snippet']['resourceId']['videoId'],'title':'','description':'',
                 'thumbnail':'','publishedAt':'','privacyStatus':''}
            try:
                vid['title']=item['snippet']['title']
            except:
                pass
            try:
                vid['description']=item['snippet']['description']
            except:
                pass
            try:
                vid['publishedAt']=item['snippet']['publishedAt']
            except:
                pass
            try:
                vid['thumbnail']=item['snippet']['thumbnails']['default']['url']
            except:
                pass
            try:
                vid['privacyStatus']=item['status']['privacyStatus']
            except:
                pass
            
            vids=vids.append(vid,ignore_index=True)
        if 'nextPageToken' in vidsFromChannel.keys():
            pageToken=vidsFromChannel['nextPageToken']
        else:
            nextPage=False
            
    vids=getVidsStats(vids)
    return vids 