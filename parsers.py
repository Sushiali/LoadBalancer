from flask import jsonify


def response_parser(item, response_code=200):

    return jsonify(item), response_code
