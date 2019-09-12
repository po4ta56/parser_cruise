#!/bin/bash

python3 -m virtualenv venv
source ./venv/bin/activate
pip3 install --no-cache-dir -r requirements.txt
