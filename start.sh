#!/bin/sh
export UID=$(id -u)
export GID=$(id -g)

docker compose down -v
docker compose up -d