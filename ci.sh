#!/bin/bash

scp -r /src/ pi@raspberrypi.local:/home/pi/bot/
ssh pi@raspberrypi.local 'bash -s' < deploy.sh
