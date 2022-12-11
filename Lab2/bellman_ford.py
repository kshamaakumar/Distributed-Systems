import math


class Bellman(object):

    def __init__(self):
    
        self.graph = {}

    def addEdge(self, fromCur, toCur, exchangeRate):
       
        if fromCur not in self.graph.keys():
            self.graph[fromCur] = {toCur: -exchangeRate}
        else:
            self.graph[fromCur][toCur] = -exchangeRate
        if toCur not in self.graph.keys():
            self.graph[toCur] = {fromCur: exchangeRate}
        else:
            self.graph[toCur][fromCur] = exchangeRate

    def removeEdge(self, fromCur, toCur):
       
        try:
            del self.graph[fromCur][toCur]
            del self.graph[toCur][fromCur]
        except:
            return

    def getCurrencyRatio(self, fromCur, toCur):
        
        return self.graph[fromCur][toCur]

    def getCurrencies(self):
        
        return list(self.graph.keys())

    def shortest_paths(self, start_vertex, tolerance=1e-12):
        
        currencies = self.graph.keys()

        dist = {v: float("Inf") for v in currencies}

        prev = {}

        dist[start_vertex] = 0
        prev[start_vertex] = None

        # Relax all edges |V| - 1 times
        for _ in range(len(currencies) - 1):
            # Update dist value and parent index of the adjacent vertices of
            # the picked vertex
            for u, uValue in self.graph.items():
                for v, w in uValue.items():
                    w = self.moneyToLog(w)
                    if dist[u] != float("Inf") and dist[u] + w < dist[v]:
                        if dist[v] - (dist[u] + w) >= tolerance:
                            dist[v] = dist[u] + w
                            prev[v] = u

        neg_edge = None  

        # Detect the negative cycle
        for u, uValue in self.graph.items():
            for v, w in uValue.items():
                w = self.moneyToLog(w)
                if dist[u] != float("Inf") and dist[u] + w < dist[v]:
                    neg_edge = (v, u)
                    return dist, prev, neg_edge

        return dist, prev, neg_edge

    @staticmethod
    def moneyToLog(w):
        """ Change currency ratio to log number"""
        if w < 0:  
            return -math.log(abs(w), 10)
        else:  
            return math.log(w, 10)