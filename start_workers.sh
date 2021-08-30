#!/bin/bash

celery -A tasks worker --loglevel=INFO &
celery -A tasks beat -S redbeat.RedBeatScheduler --max-interval 1 --loglevel=INFO &
celery -A tasks flower worker --loglevel=INFO -n flower

