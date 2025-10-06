.PHONY: up ingest etl

up:
docker compose up -d

ingest:
python -m src.ingest_yamusic

etl:
python -m src.etl.mongo_loader
