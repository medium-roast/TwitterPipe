#!/bin/sh
python twitterToSpark.py &
python sparkProcess.py &
python app.py &