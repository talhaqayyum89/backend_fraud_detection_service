import json
from flask import Blueprint, Response, jsonify
from flask_cors import cross_origin
from app.modules.processing_funcs import get_fraud_report

fraud_blueprint = Blueprint('fraud',
                            __name__,
                            url_prefix='/fraud',
                            )


@fraud_blueprint.route('/', methods=['POST'])
@cross_origin(origin='*', headers=['Content-Type', 'application/json'])
# @auth.login_required
def index():
    return jsonify({"hello": "world"})


@fraud_blueprint.route('/report', methods=['POST'])
@cross_origin(origin='*', headers=['Content-Type', 'application/json'])
# @auth.login_required
def report():
    # request_obj = request.get_json(force=True)
    # No request required
    response_obj = get_fraud_report()

    if not response_obj['status'] == 200:
        response_payload = json.dumps({"message": response_obj["predictions"]})
        return Response(response=response_payload,
                        status=response_obj['status'],
                        content_type='application/json')

    response_payload = json.dumps(response_obj)
    return Response(response=response_payload,
                    status=200,
                    content_type='application/json')
