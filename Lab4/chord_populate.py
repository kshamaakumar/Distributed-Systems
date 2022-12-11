import csv
import socket
import pickle
import json
import time
import sys

class chord_populate:
    def main(self,port,filePath):
        ip = "127.0.0.1"
        port = int(port)
        with open(filePath, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 1
            for row in csv_reader:
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                sock.connect((ip,port))
                print(f'\t{row["Player Id"]} - {row["Year"]}.')
                key = row["Player Id"] + "_" + row["Year"]
                val = json.dumps(row)
                message = "insert|" + str(key) + ":" + str(val)
                sock.sendall(pickle.dumps(message))
                line_count += 1
                sock.close()
                time.sleep(1)

if __name__ == '__main__':
    if len(sys.argv) == 3:
        chord_populate = chord_populate()
        chord_populate.main(sys.argv[1],sys.argv[2])
    else:
         print("Port and filepath not entered")