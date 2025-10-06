import json
import os
from collections import Counter
from yandex_music import Client
from datetime import datetime
from src.config import YA_TOKEN, YA_LANGUAGE, DATASET_PATH

def norm_genre(g):
    return g.strip().lower() if g else None

def main():
    client = Client(YA_TOKEN, language=YA_LANGUAGE).init()

    likes = client.users_likes_tracks()
    track_ids, like_rows = [], []

    for l in likes:
        track_ids.append(int(l.id))
        like_rows.append({
            "track_id": int(l.id),
            "liked_at": l.timestamp,
        })

    genre_counter = Counter()
    tracks_data = []

    tracks_full = client.tracks(track_ids=track_ids)
    for t in tracks_full:
        tid = int(t.id)
        title = t.title
        duration = t.duration_ms
        explicit = (t.content_warning == "explicit") or getattr(t, "explicit", None)

        artists = [{
            "id": a.id,
            "name": a.name,
            "genres": [norm_genre(x) for x in getattr(a, "genres", []) if x],
        } for a in getattr(t, "artists", [])]

        albums = [{
            "id": al.id,
            "title": al.title,
            "genre": norm_genre(getattr(al, "genre", None)),
            "release_date": getattr(al, "release_date", None),
            "year": getattr(al, "year", None),
        } for al in getattr(t, "albums", [])]

        album_genres  = [a["genre"] for a in albums if a["genre"]]
        artist_genres = []
        for a in artists:
            artist_genres.extend(a["genres"])
        genres = sorted(set([g for g in (album_genres + artist_genres) if g]))

        primary_genre = album_genres[0] if album_genres else (genres[0] if genres else None)

        release_year = None
        for al in albums:
            if al["year"]:
                release_year = al["year"]
                break

        genre_counter.update(genres)

        tracks_data.append({
            "id": tid,
            "title": title,
            "duration_ms": duration,
            "explicit": bool(explicit),
            "primary_genre": primary_genre,
            "release_year": release_year,
            "genres": genres,
            "artists": [{"id": a["id"], "name": a["name"]} for a in artists],
            "albums": albums,
        })

    genres_stats = dict(genre_counter.most_common())

    def to_dt(s):
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    likes_period = [to_dt(r["liked_at"]) for r in like_rows]
    summary = {
        "n_likes": len(like_rows),
        "n_tracks": len(tracks_data),
        "n_artists": len({a["id"] for t in tracks_data for a in t["artists"]}),
        "n_genres": len({g for t in tracks_data for g in t["genres"]}),
        "period": [min(likes_period).isoformat(), max(likes_period).isoformat()] if likes_period else None
    }

    top_artists = Counter(a["name"] for t in tracks_data for a in t["artists"]).most_common(20)
    top_genres  = Counter(g for t in tracks_data for g in t["genres"]).most_common(20)

    result = {
        "likes": like_rows,
        "tracks": tracks_data,
        "genres_stats": genres_stats,
        "summary": summary,
        "top_artists_by_likes": top_artists,
        "top_genres_by_likes": top_genres
    }

    os.makedirs(os.path.dirname(DATASET_PATH), exist_ok=True)
    with open(DATASET_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"✅ Данные сохранены в {DATASET_PATH}")

if __name__ == "__main__":
    main()
