import socket
import time
import pickle

register_end_point = ('localhost', 23232)

with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as get_registration:
    get_registration.bind(register_end_point)
    data, addr = get_registration.recvfrom(4096)
    subscription_end_point = pickle.loads(data)

with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as publish_socket:
    for n in range(10):
        message = 'message {}'.format(n).encode('utf-8')
        print('sending {!r} (even if nobody is listening)'.format(message))
        sent = publish_socket.sendto(message, subscription_end_point)
        time.sleep(1.0)

