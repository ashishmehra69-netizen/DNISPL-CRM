from simple_crm_backend import app
from flask import request, Response

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

@app.route("/api/<path:path>", methods=["OPTIONS"])
def handle_options(path):
    response = Response()
    response.status_code = 200
    return add_cors(response)

@app.route("/api/", methods=["OPTIONS"])
def handle_options_root():
    response = Response()
    response.status_code = 200
    return add_cors(response)
