#! /bin/env python3
#
# This program implements a web server that uses the Key-Value server
#
from flask import Flask


app = Flask(__name__)

@app.route('/')
def hello_worold():
    return ('Hello, World')




