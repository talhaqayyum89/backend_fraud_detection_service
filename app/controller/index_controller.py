import os
from flask import Blueprint, jsonify

template_dir = os.path.dirname(
        os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
template_dir = os.path.join(template_dir, 'templates')

static_dir = os.path.dirname(
        os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
static_dir = os.path.join(static_dir, 'static')

module = Blueprint('index',
                   __name__,
                   url_prefix='/',
                   static_folder=static_dir,
                   template_folder=template_dir
                   )


@module.route('/', methods=['GET', 'POST'])
def index():
    return jsonify({"hello": "world"})
