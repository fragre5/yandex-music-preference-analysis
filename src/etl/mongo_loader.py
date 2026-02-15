import json
import os
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:rootpass@localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "yamusic")
DATASET_PATH = os.getenv("DATASET_PATH", "../../analysis/notebooks/dataset/yandex_music_data.json")

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def ensure_indexes(db):
    # tracks
    db.tracks.create_index([("primary_genre", ASCENDING)])
    db.tracks.create_index([("release_year", DESCENDING)])
    # artists
    db.artists.create_index([("name", ASCENDING)])
    # albums
    db.albums.create_index([("genre", ASCENDING)])
    db.albums.create_index([("year", DESCENDING)])
    # likes
    db.likes.create_index([("track_id", ASCENDING), ("liked_at", ASCENDING)], unique=True)
    db.likes.create_index([("liked_at", DESCENDING)])
    # genres_stats
    # _id = genre (строка)
    # meta
    # _id = "summary"

def upsert_tracks(db, tracks):
    ops = []
    now = iso_now()
    for t in tracks:
        doc = {
            "_id": int(t["id"]),
            "title": t.get("title"),
            "duration_ms": t.get("duration_ms"),
            "explicit": bool(t.get("explicit")),
            "primary_genre": t.get("primary_genre") or (t["genres"][0] if t.get("genres") else None),
            "release_year": t.get("release_year"),
            "genres": t.get("genres", []),
            "artists": [{"id": a["id"], "name": a.get("name")} for a in t.get("artists", [])],
            "albums": [{
                "id": al["id"],
                "title": al.get("title"),
                "genre": al.get("genre"),
                "release_date": al.get("release_date"),
                "year": al.get("year"),
            } for al in t.get("albums", [])],
            "updatedAt": now
        }
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
    if ops:
        db.tracks.bulk_write(ops, ordered=False)

def upsert_artists(db, tracks):
    seen = {}
    for t in tracks:
        for a in t.get("artists", []):
            seen[a["id"]] = {"_id": a["id"], "name": a.get("name")}
    if not seen:
        return
    ops = [UpdateOne({"_id": _id}, {"$set": doc}, upsert=True) for _id, doc in seen.items()]
    db.artists.bulk_write(ops, ordered=False)

def upsert_albums(db, tracks):
    seen = {}
    for t in tracks:
        for al in t.get("albums", []):
            seen[al["id"]] = {
                "_id": al["id"],
                "title": al.get("title"),
                "genre": al.get("genre"),
                "release_date": al.get("release_date"),
                "year": al.get("year"),
            }
    if not seen:
        return
    ops = [UpdateOne({"_id": _id}, {"$set": doc}, upsert=True) for _id, doc in seen.items()]
    db.albums.bulk_write(ops, ordered=False)

def upsert_likes(db, likes):
    # unique key: (track_id, liked_at)
    ops = []
    for l in likes:
        doc = {
            "track_id": int(l["track_id"]),
            "liked_at": l["liked_at"],
        }
        ops.append(UpdateOne(
            {"track_id": doc["track_id"], "liked_at": doc["liked_at"]},
            {"$setOnInsert": doc},
            upsert=True
        ))
    if ops:
        db.likes.bulk_write(ops, ordered=False)

def upsert_genres_stats(db, genres_stats):
    ops = []
    for genre, cnt in genres_stats.items():
        ops.append(UpdateOne(
            {"_id": genre},
            {"$set": {"cnt": int(cnt)}},
            upsert=True
        ))
    if ops:
        db.genres_stats.bulk_write(ops, ordered=False)

def upsert_summary(db, data):
    doc = {
        "_id": "summary",
        "summary": data.get("summary"),
        "top_artists_by_likes": data.get("top_artists_by_likes"),
        "top_genres_by_likes": data.get("top_genres_by_likes"),
        "updatedAt": iso_now()
    }
    db.meta.update_one({"_id": "summary"}, {"$set": doc}, upsert=True)

def main():
    if not os.path.exists(DATASET_PATH):
        raise FileNotFoundError(f"Не найден файл датасета: {DATASET_PATH}")

    with open(DATASET_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    ensure_indexes(db)

    tracks = data.get("tracks", [])
    likes = data.get("likes", [])
    genres_stats = data.get("genres_stats", {})

    upsert_tracks(db, tracks)
    upsert_artists(db, tracks)
    upsert_albums(db, tracks)
    upsert_likes(db, likes)
    upsert_genres_stats(db, genres_stats)
    upsert_summary(db, data)

    print("✅ Mongo ETL complete.")
    client.close()

if __name__ == "__main__":
    main()
