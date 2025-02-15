import requests
import os


YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q=tech&type=video&key={YOUTUBE_API_KEY}"

response = requests.get(url)
print(response.json())
