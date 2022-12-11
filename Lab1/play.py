import pickle

data = [{'host': 'cs1.seattleu.edu', 'port': 21313}, {'host': 'cs2.seattleu.edu', 'port': 33313}]
p = pickle.dumps(data);
print(p)
print(pickle.loads(p))