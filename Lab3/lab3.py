import selectors
import socket
import sys
from datetime import datetime
import fxp_bytes_subscriber
from bellman_ford import Bellman

BUF_SZ = 4096  
CHECK_INTERVAL = 0.2  
TIMEOUT = 60  


class Lab3(object):

    def __init__(self, gcd_address):
    
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        self.listener, self.listener_address = self.start_a_listener()
        self.selector = selectors.DefaultSelector()
        self.latestTime = datetime(1970, 1, 1)
        self.currTimeStamp = {}  
        self.bellman_ford = Bellman()    

    def run(self):
        
        print('starting up on {} port {}'.format(*self.listener_address))
        self.selector.register(self.listener, selectors.EVENT_READ)

        serializedAdd = fxp_bytes_subscriber.serialize_address(
            self.listener_address[0], self.listener_address[1])

        self.listener.sendto(serializedAdd, self.gcd_address)

        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                data = self.receive_message()
                self.removeOldQuote()
                self.createGraph(data)
                self.arbitrage()

    @staticmethod
    def start_a_listener():

        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(('localhost', 0))
        return listener, listener.getsockname()

    def receive_message(self):
        
        try:
            data = self.listener.recvfrom(BUF_SZ)
            return fxp_bytes_subscriber.unmarshal_message(data[0])
        except ConnectionError as err:
            print('Connection error: {}'.format(err))
            return
        except Exception as err:
            
            print('Failed {}'.format(err))
            return

    def removeOldQuote(self):
        if self.currTimeStamp:
            for key, value in list(self.currTimeStamp.items()):
                if 1.5 < (datetime.utcnow() - value).total_seconds():
                    fromCur = key[0:3]
                    toCur = key[3:6]
                    print('removing stale quote for (\'{}\', \'{}\')'.
                          format(fromCur, toCur))
                    self.bellman_ford.removeEdge(fromCur, toCur)
                    del self.currTimeStamp[key]

    def createGraph(self, arr):

        for x in arr:
            print(x)
            currTime = datetime.strptime(x[0:26], '%Y-%m-%d %H:%M:%S.%f')
            if self.latestTime <= currTime:
                self.latestTime = currTime
                fromCur = x[27:30]
                toCur = x[31:34]
                exchangeRate = float(x[35:])
                self.currTimeStamp[fromCur + toCur] = currTime
                self.bellman_ford.addEdge(fromCur, toCur, exchangeRate)

    def arbitrage(self):
        # Iteratively find arbitrage with existing currencies
        for x in self.bellman_ford.getCurrencies():
            dist, prev, neg_edge = self.bellman_ford.shortest_paths(x)
            if neg_edge is not None:  # Operate if arbitrage is found
                isNegCycle = self.getCycle(dist, prev, neg_edge)
                if isNegCycle:
                    break

    def getCycle(self, dist, prev, neg_edge):
        
        cycle = []       
        v = neg_edge[1]  

        while True:
            if v is None:
                return False
            cycle.append(v)

            if len(cycle) > len(dist):
                break
            if v == neg_edge[1] and len(cycle) > 1:
                break
            v = prev[v]

        if cycle[0] != cycle[len(cycle) - 1]:
            return False

        cycle.reverse()
        return self.printResults(cycle)

    def printResults(self, cycle):

        arbitrageResult = '\tstart with {} 100\n'.format(cycle[0])
        currentPrice = 100
        for i in range(len(cycle) - 1):
            ratio = self.bellman_ford.getCurrencyRatio(cycle[i], cycle[i + 1])
            if ratio < 0: 
                ratio = abs(ratio)
            else:  
                ratio = 1 / ratio
            currentPrice = ratio * currentPrice
            arbitrageResult += '\texchange {} for {} at {} --> {} {}\n'.format(
                cycle[i], cycle[i + 1], ratio, cycle[i + 1], currentPrice)

        print('ARBITRAGE:')
        print(arbitrageResult)
        return True

if __name__ == '__main__':
    if not 3 == len(sys.argv):
        print("Usage: python3 lab3.py Host, Port")
        exit(1)

    lab3 = Lab3(sys.argv[1:3])
    lab3.run()