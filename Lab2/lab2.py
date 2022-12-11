import socket
import sys
import pickle
import selectors
from datetime import datetime
from enum import Enum


buffer = 1024
timout = 1.5
failureTimeout = 0.2

class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)
    
class Bully(object):

    def __init__(self, gcd_address, next_birthday, su_id):

        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.members = {}
        self.states = {}
        self.bully = None  # None means election is pending, otherwise this will be pid of leader
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

        def joinMessage(self):

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                message = (self.pid, self.listener_address)
                awaitingReply = True
                s.connect(self.gcd_address)
                pickledata = pickle.dumps('JOIN')
                s.sendall(pickledata)

                if awaitingReply:
                    data = s.recv(buffer)
                    unpickledata = pickle.loads(data)
                    self.members = (unpickledata,None)

        def connectToMembers(self, member):

            listener = self.members[member]
            peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer.setblocking(False)
            try:
                peer.connect(listener)
            except Exception as e:
                print('failed to connect'.format(e))
                return None

            return peer

        def election(self):

            print('Starting an election')
            self.bully = None
            #return 'unknown' if self.bully is None else ('self' if self.bully == self.pid else self.bully)
            print('The current leader is {}'.format(self.bully)
            #self.set_leader(None)
            self.set_state(State.WAITING_FOR_OK)
            currentLeader = True
            for member in self.members:
                if member > self.pid:
                    peer = self.connectToMembers(member)
                    self.set_state(State.SEND_ELECTION, peer)
                    currentLeader = False

            if currentLeader:
                print('No other bullies bigger than {}'.format(self.pid))
                self.set_leader(self.pid)
                for member in self.members:
                    if member != self.pid:
                        peer = self.connectToMembers(member)
                        self.set_state(State.SEND_VICTORY, peer)
                self.set_state(State.QUIESCENT,peer)

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
                    return

                # note the new state and adjust selector as necessary
                if peer != self and peer not in self.states:
                    peer.setblocking(False)
                    self.selector.register(peer, mask)
                elif switch_mode:
                    self.selector.modify(peer, mask)
                self.states[peer] = (state, datetime.now())

                # try sending right away (if currently writable, the selector will never trigger it)
                if mask == selectors.EVENT_WRITE:
                    self.send_message(peer)

        def run(self):
            print('Starting process')
            self.joinMessage()
            self.selector.register(self.listener, selectors.EVENT_READ)
            self.start_election()
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


if __name__ == '__main__':
    if len(sys.argv) == 5:
        # assume ISO format for DOB, e.g., YYYY-MM-DD
        dateParts = sys.argv[3].split('-')
        now = datetime.now()
        comingBirthday = datetime(now.year, int(dateParts[1]), int(dateParts[2]))
        if comingBirthday < now:
            comingBirthday = datetime(comingBirthday.year + 1, comingBirthday.month, comingBirthday.day)

    print('Next Birthday:', comingBirthday)
    SUID = int(sys.argv[4])
    print('SU ID:', SUID)
    lab2 = bully(sys.argv[1:3], comingBirthday, SUID)
    lab2.run()
    
