import requests

API_KEY = "YOUR_YOUTUBE_API_KEY"
url = "https://www.googleapis.com/youtube/v3/videoCategories"

params = {
    "part": "snippet",
    "regionCode": "ID",  # Ganti dengan kode negara yang diinginkan
    "key": API_KEY
}

response = requests.get(url, params=params).json()

for item in response.get("items", []):
    print(f"ID: {item['id']} - Nama: {item['snippet']['title']}")
