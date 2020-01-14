#! /bin/env python3
#
# This program implements a web server that uses the Key-Value server
#
#
# To start the web server from a development environment, set these 2 envars
# $ export FLASK_ENV=development
# $ export FLASK_APP=kv_web_server.py
# flask run

from flask import Flask
from flask import request

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST', 'PUT', 'DELETE'])
def hello_world():
    return f'Hello, World.  Request method is {request.method}'

