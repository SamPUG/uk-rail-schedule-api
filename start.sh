#!/bin/sh

# Run initial update (ignore curl failure for initial run)
if [ -z "$SKIP_INITIAL_UPDATE" ]; then
    ./update-schedule-feed.sh || true
fi

# Start cron daemon
crond

# Run the application
./uk-rail-schedule-api