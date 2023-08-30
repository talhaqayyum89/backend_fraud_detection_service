from flask import Flask
from flask_cors import CORS

from .controller.index_controller import module
from .controller.fraud_controller import fraud_blueprint


webapp = Flask(__name__)
CORS(webapp)
webapp.config.from_object('config.config')

# webapp.register_blueprint(module)
webapp.register_blueprint(fraud_blueprint)
