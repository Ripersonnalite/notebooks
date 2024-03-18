from threading import Lock
from flask import Flask, render_template, session
from flask_socketio import SocketIO, emit
import requests
import datetime


# Set this variable to "threading", "eventlet" or "gevent" to test the
# different async modes, or leave it set to None for the application to choose
# the best option based on installed packages.
async_mode = None

app = Flask(__name__)
socketio = SocketIO(app, async_mode=async_mode)
thread = None
thread_lock = Lock()

url = 'https://api.coinbase.com/v2/prices/btc-usd/spot'

def background_thread():
    """Example of how to send server generated events to clients."""
    count = 0
    while True:
        now = datetime.datetime.now()
        socketio.sleep(3)
        count += 1
        price = ((requests.get('https://api.coinbase.com/v2/prices/' + 'btc' + '-usd/spot')).json())['data']['amount']
        socketio.emit('my_response',
                      {'data': now.strftime("%Y-%m-%d %H:%M:%S") +' BTC price (USD): ' + price, 'count': count})

        price = ((requests.get('https://api.coinbase.com/v2/prices/' + 'eth' + '-usd/spot')).json())['data']['amount']
        socketio.emit('my_response2',
                      {'data': now.strftime("%Y-%m-%d %H:%M:%S") +' ETH price (USD): ' + price, 'count': count})
        

@app.route('/')
def index():
    return render_template('index.html', async_mode=socketio.async_mode)

@socketio.event
def my_event(message):
    session['receive_count'] = session.get('receive_count', 0) + 1
    emit('my_response',
         {'data': message['data'], 'count': session['receive_count']})

@socketio.event
def connect():
    global thread
    with thread_lock:
        if thread is None:
            thread = socketio.start_background_task(background_thread)
    emit('my_response', {'data': 'Connected!', 'count': 0})

if __name__ == '__main__':
    socketio.run(app)