#!/bin/sh

# Run initial update (ignore curl failure for initial run)
./update-schedule-feed.sh || true

# Start cron daemon
crond

# Run the application
./uk-rail-schedule-api