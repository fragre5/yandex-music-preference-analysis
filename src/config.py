import os
from dotenv import load_dotenv

load_dotenv()

YA_TOKEN     = os.getenv("YA_TOKEN")
YA_LANGUAGE  = os.getenv("YA_LANGUAGE", "ru")
DATASET_PATH = os.getenv("DATASET_PATH", "./dataset/yandex_music_data.json")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:rootpass@localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB", "yamusic")