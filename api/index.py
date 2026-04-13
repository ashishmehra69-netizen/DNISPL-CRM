from simple_crm_backend import app

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

@app.route("/api/<path:path>", methods=["OPTIONS"])
def handle_options(path):
    from flask import Response
    return add_cors(Response())
