import socket
import pickle

register_end_point = ('localhost', 23232)

with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as subscription_socket:
    subscription_socket.bind(('localhost', 0))
    my_end_point = subscription_socket.getsockname()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as register_socket:
        register_socket.sendto(pickle.dumps(my_end_point), register_end_point)
    for i in range(10):
        print('\nblocking, waiting to receive message')
        data, addr = subscription_socket.recvfrom(4096)
        print(data, 'from', addr)
