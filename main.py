import json
import os
import datetime
import tempfile
import googleapiclient.discovery
import googleapiclient.errors
from google.oauth2 import service_account
from pytubefix import YouTube 
import dotenv

dotenv.load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
PARENT_FOLDER_ID = os.getenv("PARENT_FOLDER_ID")

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'service_account.json'
OUTPUT_DIR = "videos"
LAST_VIDEO_N = 3


def is_older_than_days(date_str, days):
    created_time = datetime.datetime.fromisoformat(date_str[:-1])
    return created_time < datetime.datetime.now() - datetime.timedelta(days=days)


def delete_video_from_drive(video_id):
    service = get_drive_service()
    try:
        service.files().delete(fileId=video_id).execute()
        print(f"Deleted video from Drive: {video_id}")
    except googleapiclient.errors.HttpError as e:
        print(f"Error deleting video from Drive: {e}")


def delete_old_videos(days_to_keep=14):
    service = get_drive_service()
    # print all files names and upload dates
    results = service.files().list(
        q=f"'{PARENT_FOLDER_ID}' in parents",
        spaces='drive',
        fields='files(id, name, createdTime)'
    ).execute()
    items = results.get('files', [])
    for item in items:
        created_time = item['createdTime']
        if is_older_than_days(created_time, days_to_keep):
            print(f"Deleting old video: {item['name']} created at {created_time}")
            delete_video_from_drive(item['id'])


def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = googleapiclient.discovery.build('drive', 'v3', credentials=creds)
    return service


def get_latest_video_url(channel_id, last_n=1):
    """
    Get the URL of the latest video from a YouTube channel using the YouTube Data API.
    
    Args:
        api_key: Your YouTube Data API key
        channel_url: The URL of the YouTube channel
    
    Returns:
        The URL of the latest video or None if not found
    """
    # Extract channel ID or username from URL
    channel_id
    if not channel_id:
        return None, "Could not extract channel identifier from URL"
    
    # Initialize the YouTube API client
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    
    try:
        # Get the uploads playlist ID for the channel
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        
        if not response.get("items"):
            return None, f"No channel found with ID: {channel_id}"
        
        # Get the uploads playlist ID
        uploads_playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        
        # Get the most recent video from the uploads playlist
        request = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=last_n
        )
        response = request.execute()
        
        if not response.get("items"):
            return None, "Channel has no videos"
        
        # Extract video information
        videos = []
        for i in range(last_n):
            video_id = response["items"][i]["snippet"]["resourceId"]["videoId"]
            video_title = response["items"][i]["snippet"]["title"]
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            videos.append((video_url, video_title))
        
        return videos
    
    except googleapiclient.errors.HttpError as e:
        return None, f"API Error: {e}"
    except Exception as e:
        return None, f"An unexpected error occurred: {e}"


def download_video(video_url, output_path):
    yt = YouTube(video_url)
    streams = yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution')
    # find the stream with the resolution closest to 480p
    stream = streams.filter(res='480p').first()
    if stream is None:
        stream = streams.first()
    output_dir_path = os.path.dirname(output_path)
    file_name = os.path.basename(output_path)
    stream.download(output_path=output_dir_path, filename=file_name)


def download_channels_list_from_drive():
    service = get_drive_service()
    # get file named "channels.json" from drive ( in PARENT_FOLDER_ID)
    quary = f"name='channels.json' and '{PARENT_FOLDER_ID}' in parents"
    results = service.files().list(
        q=quary,
        spaces='drive',
        fields='files(id, name)'
    ).execute()
    items = results.get('files', [])
    if not items:
        print('file channels.json not found')
        return None
    else:
        file_id = items[0]['id']
        request = service.files().get_media(fileId=file_id)
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, 'channels.json')
            with open(file_path, 'wb') as f:
                downloader = googleapiclient.http.MediaIoBaseDownload(f, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    print(f"Download {int(status.progress() * 100)}%.")
            with open(file_path, 'r') as f:
                channels = json.load(f)
            return channels


def upload_file(file_path ,file_name):
    service = get_drive_service()
    file_metadata = {
        'name' : file_name,
        'parents' : [PARENT_FOLDER_ID]
    }
    media = googleapiclient.http.MediaFileUpload(file_path, resumable=True)
    request = service.files().create(
        body=file_metadata,
        media_body=media
    ).execute()


def main():
    delete_old_videos(days_to_keep=14)
    print(datetime.datetime.now())

    channels = download_channels_list_from_drive()
    if channels is None:
        print("No channels found")
        return
    
    with open("past_videos.json","r") as f:
        past_videos = json.load(f)

    for channel_id in channels:
        try:
            videos = get_latest_video_url(channel_id, LAST_VIDEO_N)
            if not videos:
                continue
            for video_url, video_title in videos:
                if video_url in past_videos:
                    print(f"Skipping: {video_title}")
                    continue
                try:
                    file_name = f"{video_title}.mp4"
                    file_path = os.path.join(OUTPUT_DIR, file_name)
                    download_video(video_url, file_path)
                    print(f"Downloaded: {video_title}")

                    upload_file(file_path, file_name)
                    os.remove(file_path)
                    past_videos.append(video_url)
                    print(f"Uploaded: {video_title}")
                except Exception as e:
                    print(f"Error: {e}")
                    
        except Exception as e:
            print(f"Error: {e}")

    with open("past_videos.json","w") as f:
        json.dump(past_videos, f, indent=4)

if __name__ == "__main__":
    main()
