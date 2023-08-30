from app import webapp
from config import config
import warnings
warnings.filterwarnings('ignore')

if __name__ == '__main__':
    print("webapp started.......")
    webapp.run(host=config.app_host, port=config.app_port, debug=False)
