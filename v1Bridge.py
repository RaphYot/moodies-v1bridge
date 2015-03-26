from functools import partial
import json
import logging
import sys
import time
import types

import pusherclient


# Configuration
SLEEPTIME=1
APPKEY = '2c987384b72778026687'
SECRET = '8440acd6ba1e0bfec3d4'
USERDATA = {
  'user_id': 'moodies-v1Bridge',
  'user_info': {
      'name': 'Moodies Server'
    }
}


class MoodiesBridge:

    """
    Main server class, open a websocket port and spawn new processes for moodiesV1 hardware
    """

    def __init__(self, port):
        self.logger = logging.getLogger('moodies.MoodiesBridge')
        self.logger.info('Starting Moodies v1Bridge server')
        self.killed = False
        self.pusher = None

    def run_forever(self):
        """
        Establish the connection to Pusher and keep the MoodiesServer class
        alive until killed
        """
        self.logger.info('Entering server loop')
        while not self.killed:
            time.sleep(SLEEPTIME)

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
        pusher_channel = self.pusher.subscribe('presence-moodies')
        self._setup_pusher_channel_callbacks(pusher_channel)

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
        pass

    def _callback_leaving_member(self, msg, channel_name):
        """
        Remove users from channel users list.
        """
        pass

class Message:

    """
    Parse a the json string received in pusher message data
    """

    def __init__(self, user_id=None, value=None):
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
    parser.add_argument('-p','--port',
        help='Web socket port',
        action='store_const',
        dest='port'
    )
    args = parser.parse_args()
    return args

def main(args):
    moodies_bridge = MoodiesBridge(args.port)
    moodies_bridge.run_forever()

if __name__=='__main__':
    args = parse_args()
    start_logger(args)
    main(args)
