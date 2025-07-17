from flask import Flask
import os

app = Flask(__name__)

@app.route('/')
def index():
    return 'Web App with Python Flask!'

listen_port = os.getenv('X_ZOHO_CATALYST_LISTEN_PORT', 9000)
app.run(host='0.0.0.0', port = listen_port)
