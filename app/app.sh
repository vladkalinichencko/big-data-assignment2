#!/bin/bash

service ssh restart

bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
venv-pack -f -o .venv.tar.gz

bash prepare_data.sh

bash index.sh /data

bash search.sh "world war history"
bash search.sh "music album rock"
bash search.sh "united states president"
