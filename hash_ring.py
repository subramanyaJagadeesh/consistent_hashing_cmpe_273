import hashlib
from bisect import bisect, insort

class HashRing:
  def __init__(self, nodes=None, num_replicas=3):
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
      self.keys[node_hash] = []
      self.nodes.add(node)
      insort(self.ring, node_hash)
      node_index = bisect(self.ring, node_hash)

      if node_index == len(self.ring)-1:
        node_index = 0
      keys_to_rehash = self.keys.get(self.ring[node_index], [])
      if keys_to_rehash:
        self.keys[self.ring[node_index]] = []

        for key in keys_to_rehash:
          self.add_key(key, True)
  
  def add_key(self, key, is_rehash=False):
    """Adds a key to a node in hash_ring"""
    key_hash = ''
    if(not is_rehash):
      key_hash = self._hash(key)
    else:
      key_hash = key
    for node_hash in self.ring:
      if(key_hash > node_hash):
        self.keys[node_hash].append(key_hash)
        return
    self.keys[self.ring[0]].append(key_hash)

  
  def remove_node(self, node):
    """Removes a node and its replicas from the hash ring."""
    
    for i in range(self.num_replicas):
      #get virtual node
      replica_key = f"{node}-{i}"

      #get hash of virtual node
      node_hash = self._hash(replica_key)

      #get index of node_hash from ring
      node_index = bisect(self.ring, (node_hash, node))
      
      #get list of keys which will be rehashed after removing this virtual node
      keys_to_rehash = self.keys.get(self.ring[node_index], [])

      #remove the keys of the virtual node of the "to be rehased" keys from the key list 
      if keys_to_rehash:
        self.keys[self.ring[node_index]] = []

      #remove the virtual node
      self.ring.remove((node_hash, node))

      #rehash the keys and add them to 
      for key in keys_to_rehash:
        self.add_key(key)
    self.nodes.remove(node)

  def remove_key(self, key):
    """Removes a key the virtual node in hash ring."""
    key_hash = self._hash(key)
    for node in self.nodes:
      node_hash = self._hash(node)
      if(key_hash > node_hash):
        self.keys[node_hash] = (self.keys.get(node_hash)).remove(key_hash)


  def get_node(self, key):
    """Returns the node to which the given key is mapped."""
    if not self.ring:
        return None
    key_hash = self._hash(key)
    index = bisect(self.ring, key_hash)
    return self.ring[index % len(self.ring)][1]
  
  def get_key(self, node):
      """Returns the keys associated with a given virtual node."""
      node_hash = self._hash(node)
      return self.keys.get(node_hash, [])


def main():
  hashRing = HashRing()
  hashRing._init_([], 2)
  hashRing.add_node('A')
  hashRing.add_node('B')

  print(hashRing.ring)

  hashRing.add_key("Apache")
  hashRing.add_key("Arrow")
  hashRing.add_key("Flight123")

  print(hashRing.keys)

  '''
  hashRing.add_node('C')
  hashRing.add_key("Consistent")
  hashRing.add_key("Hashing")
  hashRing.add_key("Algorithm")

  print(hashRing.keys)

  hashRing.remove_key("Arrow")
  
  print(hashRing.keys)
  hashRing.remove_node('B')
  print(hashRing.keys)
  '''
if __name__=="__main__": 
    main()
