import pickle
import selectors
import socket
import sys
from datetime import datetime
from enum import Enum

BUFFER = 4096
CHECK_INTERVAL = 0.2
FAILURE_TIMEOUT = 1.5
LISTENER_TIMEOUT = 100

class State(Enum):

    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'
    WAITING_FOR_ANY_MESSAGE = 'WAITING'

    def is_incoming(self):
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)

class Lab2(object):

    def __init__(self, gcd_address, next_birthday, su_id):
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.members = {}
        self.states = {}
        self.bully = None  # None means election is pending, otherwise this will be pid of leader
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    def main(self):
        print('STARTING WORK')
        print(self.pid ,self.listener_address)
        self.join_group()
        self.selector.register(self.listener, selectors.EVENT_READ)
        self.start_election('on joining the group')
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeouts()

    def accept_peer(self):
        try:
            peer, _addr = self.listener.accept()
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, peer)
        except Exception as err:
            print(err)

    def send_message(self, peer):

        state = self.get_state(peer)
        try:
            self.send(peer, state.value, self.members)

        except ConnectionError as e:
            self.set_quiescent(peer)
            print(e)
            return

        except Exception as e:
            print(e)
            if self.is_expired(peer):
                print('connection time out')
                self.set_quiescent(peer)
            return

        if state == State.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, peer, switch_mode=True)
        else:
            self.set_quiescent(peer)

    def receive_message(self, peer):
        try:
            messenger, their_idea = self.receive(peer)

        except ConnectionError as e:
            print('closing connection:', e)
            self.set_quiescent(peer)
            return

        except Exception as e:
            print('failed', e)
            return

        self.update_members(their_idea)

        if message_name == 'COORDINATOR':
            self.set_quiescent(peer)
            self.set_quiescent()
            self.set_leader('somebody else')

        elif message_name == 'ELECTION':
            self.set_state(State.SEND_OK, peer)
            if self.is_election_in_progress():
                return
            else:
                self.start_election('Got a VOTE')

        elif message_name == 'OK':
            if self.get_state() == State.WAITING_FOR_OK:
                self.set_state(State.WAITING_FOR_VICTOR)
            self.set_quiescent(peer)

    def check_timeouts(self):
        if self.is_expired():
            if self.get_state() == State.WAITING_FOR_OK:
                self.declare_victory('connection timed out waiting for any OK')
            else:
                self.start_election('connection timed out waiting for COORDINATION message')

    def get_connection(self, member):
        listener = self.members[member]
        peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer.setblocking(False)
        try:
            peer.connect(listener)

        except BlockingIOError:
            pass

        except Exception as e:
            print('FAILURE: get connection failed: ', e)
            return None

        return peer

    def is_election_in_progress(self):
        return self.bully is None

    def is_expired(self, peer=None, threshold=FAILURE_TIMEOUT):
        self_state, when = self.get_state(peer, detail=True)
        if self_state == State.QUIESCENT:
            return False
        waited = datetime.now() - when
        waited_seconds = waiter.total_seconds()
        return waited_seconds > threshold

    def set_leader(self, new_leader):
        self.bully = new_leader
        print('Leader is now', self.bully)

    def get_state(self, peer=None, detail=False):
        if peer is None:
            peer = self
        if peer in self.states:
            status = self.states[peer]
        else:
            status = (State.QUIESCENT, None)
        return status if detail else status[0]

    def set_state(self, state, peer=None, switch_mode=False):
        if peer is None:
            peer = self

        if state.is_incoming():
            mask = selectors.EVENT_READ
        else:
            mask = selectors.EVENT_WRITE

        if state == State.QUIESCENT:
            if peer in self.states:
                if peer != self:
                    self.selector.unregister(peer)
                del self.states[peer]
            if len(self.states) == 0:
                print('leader is ', self.bully)
            return

        if switch_mode:
            self.selector.modify(peer, mask)
        elif peer != self and peer not in self.states:
            peer.setblocking(False)
            self.selector.register(peer, mask)

        self.states[peer] = (state, datetime.now())

        if mask == selectors.EVENT_WRITE:
            self.send_message(peer)

    def set_quiescent(self, peer=None):
        self.set_state(State.QUIESCENT, peer)

    def start_election(self, reason):
        print('Start election because', reason)
        self.set_leader(None)
        self.set_state(State.WAITING_FOR_OK)
        ibully = True
        for member in self.members:
            if member > self.pid:
                peer = self.get_connection(member)
                if peer is None:
                    continue
                self.set_state(State.SEND_ELECTION, peer)
                ibully = False
        if ibully:
            self.declare_victory('I AM THE BULLY!')

    def declare_victory(self, reason):
        print('declaring the winner')
        self.set_leader(self.pid)
        for member in self.members:
            if member != self.pid:
                peer = self.get_connection(member)
                if peer != None:
                    self.set_state(State.SEND_VICTORY, peer)
        self.set_quiescent()

    def update_members(self, membership_idea):
        if membership_idea is not None:
            for m in membership_idea:
                self.members[m] = membership_idea[m]

    @classmethod
    def send(cls, peer, message_name, message_data=None, wait_for_reply=False, buffer_size=BUFFER):
        if message_data is None:
            message = message_name
        else:
            message = (message_name, message_data)
        peer.sendall(pickle.dumps(message))
        if wait_for_reply:
            return cls.receive(peer, buffer_size)

    @staticmethod
    def receive(peer, buffer_size=BUFFER):
        packet = peer.recv(buffer_size)
        if not packet:
            raise ValueError('socket closed')
        data = pickle.loads(packet)
        if type(data) == str:
            data = (data, None)
        return data

    @staticmethod
    def start_a_server():
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(('localhost', 0))
        listener.listen(LISTENER_TIMEOUT)
        listener.setblocking(False)
        listener_address =listener.getsockname()
        return listener, listener_address

    def join_group(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd:
            message_data = (self.pid, self.listener_address)
            gcd.connect((self.gcd_address[0], self.gcd_address[1]))
            self.members = self.send(gcd, 'JOIN', message_data, wait_for_reply=True)

if __name__ == '__main__':
    if len(sys.argv) != 5:
            print("Usage: python lab2.py GCD_HOST GCD_PORT SU_ID DOB")
            exit(1);

    print(sys.argv)
    gcd_address = (sys.argv[1],int(sys.argv[2]))
    time = sys.argv[4]

    print(gcd_address[1])
    spl = (sys.argv[4]).split("-")
    next_birthday = datetime(int(spl[0]),int(spl[1]),int(spl[2]))
    su_id = int(sys.argv[3])

    l = Lab2(gcd_address,next_birthday,su_id)
    l.main()
