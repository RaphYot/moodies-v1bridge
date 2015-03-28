import json
import logging
from multiprocessing import Queue
import os
import socket
import sys
from threading import Thread
import types

import pusherclient

class V1Button:

    def __init__(self, name, user_id):
        self.logger = logging.getLogger('moodies.V1Button')
        self.name = name
        self.user_id = user_id
        self.conn = None
        self.queue = None
        self.connected = False

    def connect(self, conn, queue):
        self.conn = conn
        self.queue = queue
        self.connected = True

    def listen(self):
        self.logger.info('Starting listening mode of new HW - {}'.format(self.name))
        while self.connected:
            try:
                data = self.conn.recv(1024)
                if not data:
                    raise socket.error('No data, connection closed?')
                self._handle(data.strip())
            except socket.timeout:
                self.logger.debug('Got a socket timeout in conn.recv for {}'.format(self.name))
                continue
            except socket.error:
                self.logger.error('Got a socket error in conn.recv for {}'.format(self.name))
                break
        self.conn.close()
        self.connected = False
        self.logger.info('Hardware {} socket closed'.format(self.name))

    def _handle(self, data):
        self.logger.info('Hardware {} received data: {}'.format(self.name, data))
        self.queue.put(Message(event_type='debug', user_id=self.user_id, value=data))


# Configuration
SLEEPTIME=1
APPKEY = '2c987384b72778026687'
SECRET = '8440acd6ba1e0bfec3d4'
USERDATA = {
  'user_id': 'moodies-v1Bridge',
  'user_info': {
      'name': 'Moodies v1Bridge'
    }
}
HARDWARE = {
    '1': V1Button('button_debug', 'user_debug')
}


class MoodiesBridge:

    """
    Connect to pusher and open a socket connection to listen to moodiesV1 prototypes buttons.
    Forward pusher messages to the buttons, and buttons messages to pusher.
    """

    def __init__(self, port):
        self.logger = logging.getLogger('moodies.MoodiesBridge')
        self.logger.info('Starting Moodies v1Bridge server')
        self.killed = False
        self.pusher = None
        self.queue = Queue()
        self.port = port

    def start(self):
        self._connect_to_pusher()
        self._create_and_start_socket()
        queue_thread = Thread(target=self._queue_worker)
        queue_thread.daemon = True
        queue_thread.start()
        self._socket_listerner_loop()

    def _create_and_start_socket(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.socket.bind(('', self.port))
            self.logger.info('HW Socket bind complete on port {}'.format(self.port))
            self.socket.listen(1)
            self.logger.info('HW Socket listening')
        except socket.error , msg:
            self.logger.error('Socket binding failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()

    def _socket_listerner_loop(self):
        while not self.killed:
            new_socket, source_address = self.socket.accept()
            self.logger.info('HW Connection from {}'.format(source_address))
            new_hardware = self._create_hw(new_socket, source_address)
            if new_hardware:
                hw_thread = Thread(target=new_hardware.listen)
                hw_thread.daemon = True
                hw_thread.start()

    def _create_hw(self, conn, addr, trial=0):
        """
        Start the communication with the new hardware by sending "ID"
        Expect new hardware to send it's ID, if not, close the connection
        """
        def close_conn(conn, message):
            conn.send(message)
            conn.close()
            return False

        if trial > 2:
            return close_conn(conn, 'NO ID RECEIVED')

        try:
            conn.sendall('ID\n')
            data = conn.recv(256)
        except (socket.timeout, socket.error) as e:
            self.logger.debug('Error while waiting ID: {}'.format(e))
            return False

        if data[:2]=='ID':
            hw_id = data[3:].strip()
            if hw_id in HARDWARE:
                hw = HARDWARE[hw_id]
                hw.connect(conn, self.queue)
                return hw
            else:
                self.logger.error('ERROR in _create_hw: Hardware {} at {} is not in DB!'.format(hw_id, addr))
                return close_conn(conn, 'UNAUTHORIZED')
        else:
            self.logger.error('Message {} was invalid for {}: {}'.format(trial, addr, data))
            return self._create_hw(conn, addr, trial+1)

    def _connect_to_pusher(self):
        """
        Establish the connection to pusher, and configure the callback function
        once connected
        """
        self.pusher = pusherclient.Pusher(APPKEY, secret=SECRET, user_data=USERDATA)
        self.pusher.connection.bind('pusher:connection_established', self._callback_connection_estabished)
        self.pusher.connect()
        self.logger.info('Pusher connection established')


    def _callback_connection_estabished(self, data):
        """
        Callback to subribe to channels when receiving pusher:connection_established,
        needed as we can't subscribe until we are connected.
        """
        self.logger.debug('Callback pusher:connection_established - {}'.format(data))
        self.pusher_channel = self.pusher.subscribe('presence-moodies')
        self._setup_pusher_channel_callbacks(self.pusher_channel)

    def _setup_pusher_channel_callbacks(self, pusher_channel):
        """
        Configure the config channel callbacks
        """
        pusher_channel.bind('pusher_internal:member_added', self._callback_joining_member)
        pusher_channel.bind('pusher_internal:member_removed', self._callback_leaving_member)

    def _callback_joining_member(self, msg, channel_name):
        """
        Create a new MoodiesUser and store it for the first time we see the user.
        Append the user to the channel list of users.
        """
        self.logger.debug('{} joined channel'.format(msg.user_id))

    def _callback_leaving_member(self, msg, channel_name):
        """
        Remove users from channel users list.
        """
        self.logger.debug('{} left channel'.format(msg.user_id))

    def _queue_worker(self):
        """
        Dequeue the message from thread queue and process them
        """
        while not self.killed:
            message = self.queue.get()
            self.logger.info('Got new message from hw thread: {} - {}'.format(message.event_type, message.to_dict()))
            self.pusher_channel.trigger(message.event_type, message.to_dict())

class Message:

    """
    Parse a the json string received in pusher message data
    """

    def __init__(self, event_type=None, user_id=None, value=None):
        self.event_type = event_type
        self.value = value
        self.user_id = user_id

    def feed_with_json(self, msg):
        assert type(msg) in types.StringTypes, 'Message instance did not receive a String'
        msg = json.loads(msg)
        self.value = self._get_json_val('value', msg)
        self.user_id = self._get_json_val('user_id', msg)

    def to_dict(self):
        return {
                'value': self.value
                , 'user_id': self.user_id
        }

    def _get_json_val(self, key, json_msg):
        if key in json_msg:
            return json_msg[key]
        else:
            return None

def start_logger(args):
    module_logger = logging.getLogger('moodies')
    #formatter = logging.Formatter('%(asctime)s - %(name)s.%(lineno)d - %(levelname)s - %(message)s')
    formatter = logging.Formatter('[%(levelname)8s] %(name)s.%(lineno)d --- %(message)s')
    ch = logging.StreamHandler(sys.stdout)

    ch.setFormatter(formatter)
    module_logger.addHandler(ch)

    module_logger.setLevel(args.loglevel)

    # Disable all other logging spurious messages "No handler for"
    logging.getLogger().addHandler(logging.NullHandler())


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(
        description='Moodies server listening to Pusher message and acting on them'
    )
    parser.add_argument('-d', '--debug',
        help='Setup debug loging',
        action='store_const',
        dest='loglevel',
        const=logging.DEBUG,
        default=logging.WARNING
    )
    parser.add_argument('-v','--verbose',
        help='Setup verbose loging (less than debug)',
        action='store_const',
        dest='loglevel',
        const=logging.INFO
    )
    args = parser.parse_args()
    return args

def main():
    moodies_bridge = MoodiesBridge(port = int(os.environ.get('PORT', 5000)) )
    moodies_bridge.start()

if __name__=='__main__':
    args = parse_args()
    start_logger(args)
    main()
