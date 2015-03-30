#!/usr/bin/env python

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
        self.logger = logging.getLogger('moodies.V1Button.{}'.format(user_id))
        self.name = name
        self.user_id = user_id
        self.conn = None
        self.pusher = None
        self.connected = False

    def connect(self, conn):
        conn.settimeout(60)
        self.conn = conn
        self._connect_to_pusher()
        self.connected = True

    def listen(self):
        self.logger.info('Starting listening mode of new HW - {}'.format(self.name))
        while self.connected:
            try:
                data = self.conn.recv(1024)
                if not data:
                    raise socket.error('No data, connection closed?')
                self._handle_tcp_data(data.strip())
            except socket.timeout:
                self.logger.debug('Got a socket timeout in conn.recv for {}'.format(self.name))
                if not self._test_connection():
                    self.logger.error('Test connection failed')
                    break
            except socket.error:
                self.logger.error('Got a socket error in conn.recv for {}'.format(self.name))
                break
            except Exception as e:
                # Need to close the connection, so catchall
                self.logger.exception('Got an uncatched error, {}'.format(e))
                break
        self.disconnect()

    def disconnect(self):
        self.connected = False
        self.conn.close()
        self.logger.info('Hardware {} socket closed'.format(self.name))
        self.pusher.disconnect()
        self.logger.info('Hardware {} disconnected from pusher'.format(self.name))

    def send(self, msg):
        try:
            self.conn.sendall(msg)
        except socket.error:
             self.logger.error("Socket connection issue, killing {}".format(self._id))
             self.disconnect()

    def _test_connection(self):
        try:
            self.conn.send('ID\n')
            data = self.conn.recv(256)
        except (socket.timeout, socket.error) as e:
            self.logger.debug('Error while waiting ID: {}'.format(e))
            return False
        return data[:2]=='ID'

    def _handle_tcp_data(self, data):
        self.logger.info('Hardware {} received data: {}'.format(self.name, data))
        if data.split(' ')[0] == 'BP':
            self.logger.info('Sending client-button-pushed to pusher')
            message = Message(event_type='client-button-pushed', user_id=self.user_id, value=data.split()[1])
            self.pusher_channel.trigger(message.event_type, message.to_dict())


    def _connect_to_pusher(self):
        """
        Establish the connection to pusher, and configure the callback function
        once connected
        """
        USERDATA = {
          'user_id': self.user_id,
          'user_info': {
              'name': self.name
            }
        }
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
        self._setup_pusher_channel_callbacks()

    def _setup_pusher_channel_callbacks(self):
        """
        Configure the config channel callbacks
        """
        self.pusher_channel.bind('pusher_internal:member_added', self._callback_joining_member)
        self.pusher_channel.bind('pusher_internal:member_removed', self._callback_leaving_member)
        self.pusher_channel.bind('client-play-melody', self._callback_play_melody)
        self.pusher_channel.bind('client-new-color', self._callback_new_color)
        self.pusher_channel.bind('client-text-message', self._callback_text_message)

    def _callback_play_melody(self, msg):
        message = Message()
        message.feed_with_json(msg)
        self.logger.debug('Play melody: {}'.format(message.value))
        self.send('PM {}\n'.format(message.value))

    def _callback_new_color(self, msg):
        message = Message()
        message.feed_with_json(msg)
        self.logger.debug('New color: {}'.format(message.value))
        self.send('BC {}\n'.format(message.value))

    def _callback_text_message(self, msg):
        message = Message()
        message.feed_with_json(msg)
        self.logger.debug('Text message: {}'.format(message.value))
        self.send('DT {}\n'.format(message.value.upper()))

    def _callback_joining_member(self, msg):
        """
        Create a new MoodiesUser and store it for the first time we see the user.
        Append the user to the channel list of users.
        """
        pass

    def _callback_leaving_member(self, msg):
        """
        Remove users from channel users list.
        """
        pass


# Configuration
SLEEPTIME=1
APPKEY = '2c987384b72778026687'
SECRET = '8440acd6ba1e0bfec3d4'

HARDWARE = {
    '1': V1Button('button_debug', 'user_debug'),
    '5261706872': V1Button('button_did', 'dcolens')
}


class MoodiesBridge:

    """
    Open a socket connection to listen to moodiesV1 prototypes buttons.
    And create a new button when a new HW register.
    """

    def __init__(self, port):
        self.logger = logging.getLogger('MoodiesBridge')
        self.logger.info('Starting Moodies v1Bridge server')
        self.killed = False
        self.port = port

    def start(self):
        self._create_and_start_socket()
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
                hw.connect(conn)
                return hw
            else:
                self.logger.error('ERROR in _create_hw: Hardware {} at {} is not in DB!'.format(hw_id, addr))
                return close_conn(conn, 'UNAUTHORIZED')
        else:
            self.logger.error('Message {} was invalid for {}: {}'.format(trial, addr, data))
            return self._create_hw(conn, addr, trial+1)

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
    module_logger = logging.getLogger()
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
    #moodies_bridge = MoodiesBridge(port=int(os.environ.get('PORT', 4123)) )
    moodies_bridge = MoodiesBridge(port=4123)
    moodies_bridge.start()

if __name__=='__main__':
    args = parse_args()
    start_logger(args)
    main()
