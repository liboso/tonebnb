import os
from flask import Flask, render_template
from models import db
from api import listing_api, heatmap_api, safety_info_api


_file_dir = os.path.dirname(os.path.realpath(__file__))
app = Flask(__name__, template_folder=os.path.join(_file_dir, "static/templates"))

db_host = os.getenv('POSTGRES_HOST', '10.0.0.5')
db_port = os.getenv('POSTGRES_PORT', '5432')
db_user = os.getenv('POSTGRES_USER', 'sa')
db_pwd = os.getenv('POSTGRES_PWD', 'sa')

app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{db_user}:{db_pwd}@{db_host}:{db_port}/tonebnb'

#db.init_app(app)

#app.register_blueprint(listing_api, url_prefix='/api/listings')
#app.register_blueprint(heatmap_api, url_prefix='/api/heatmap')
#app.register_blueprint(safety_info_api, url_prefix='/api/safetyinfo')


@app.after_request
def add_header(r):
    """
    Add headers to force browser to not cache response
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r


@app.route('/')
def index():
    return render_template('map.html', title='Tone booking')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
