import socket
import sys
import pickle


class Client:

    def clientcall(self):
        if len(sys.argv) != 3:
            print("Usage: python client.py HOST PORT")
            exit(1)

        host = sys.argv[1]
        port = int(sys.argv[2])
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            pickledata = pickle.dumps('JOIN')
            s.sendall(pickledata)
            data = s.recv(1024)
            unpickledata = pickle.loads(data)
        print('Received', repr(unpickledata))

        for word in unpickledata:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((word['host'], word['port']))
                    print('HELLO to', word)
                    picklenext = pickle.dumps('HELLO')
                    s.sendall(picklenext)
                    picklenext = s.recv(1024)
                    unpicklenext = pickle.loads(picklenext)
                    print(repr(unpicklenext))
            except Exception as e:
                print('failed to connect', repr(e))


if __name__ == '__main__':
    call = Client()
    call.clientcall()
