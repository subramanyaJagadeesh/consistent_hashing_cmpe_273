import hashlib
from bisect import bisect, insort

class HashRing:
  def _init_(self, nodes=None, num_replicas=3):
    # Number of virtual nodes for each physical node
    self.num_replicas = num_replicas
    #Physical nodes
    self.nodes = set()
    #Hash ring
    self.ring = []
    #To keep a log of hash_keys stored in a particular virtual node
    self.keys = {}

    if nodes:
      for node in nodes:
        self.add_node(node)
        self.keys[self._hash(node)] = []

  def _hash(self, key):
    """Returns a hash for the given key."""
    return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

  def add_node(self, node):
    """Adds a node to the hash ring with its replicas."""
    for i in range(self.num_replicas):
      replica_key = f"{node}-{i}"
      node_hash = self._hash(replica_key)
      self.nodes.add(node)
      insort(self.ring, (node_hash, node))

    node_index = bisect(self.ring, node_hash)

    if node_index == len(self.ring)-1:
      node_index = 0
    else:
      node_index += 1

    keys_to_rehash = self.keys.get(self.ring[node_index], [])
    if keys_to_rehash:
      self.keys[self.ring[node_index]] = []

      for key in keys_to_rehash:
        self.add_key(key)
  
  def add_key(self, key):
    """Adds a key to a node in hash_ring"""
    key_hash = self._hash(key)
    for node in self.nodes:
      node_hash = self._hash(node)
      if(key_hash > node_hash):
        self.keys[node_hash] = self.keys.get(node_hash).append(key_hash)