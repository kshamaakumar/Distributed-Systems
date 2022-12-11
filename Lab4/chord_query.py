import sys
import socket
import pickle

class chord_query:
    def main(self,port,playerName,year):
        ip = "127.0.0.1"
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((ip,int(port)))
        key = playerName + "_" + year
        message = "search|" + str(key)
        sock.sendall(pickle.dumps(message))
        data = sock.recv(1048576)
        data = pickle.loads(data)
        print(data)

if __name__ == '__main__':
    if len(sys.argv) == 4:
        Chord_query = chord_query()
        Chord_query.main(sys.argv[1],sys.argv[2],sys.argv[3])
    else:
        print("Port, player name and year not entered")