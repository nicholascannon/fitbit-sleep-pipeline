#!/usr/bin/env bash
# Start Postgres data warehouse in docker
docker run --rm -d -p 5432:5432 --name sleep_dw postgres
sleep 1 # give the container time to boot
cat scripts/setup_dw.sql | docker exec -i sleep_dw psql -U postgres
