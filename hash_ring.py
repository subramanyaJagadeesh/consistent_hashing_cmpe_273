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
      self.keys[node_hash] = []
      self.nodes.add(node)
      insort(self.ring, (node_hash, node))
      
      node_index = bisect(self.ring, (node_hash, node))

      if node_index == len(self.ring):
        node_index = 0
      keys_to_rehash = self.keys.get(self.ring[node_index])
      if keys_to_rehash:
        self.keys[self.ring[node_index]] = []
        for key in keys_to_rehash:
          self.add_key(key)
  
  def add_key(self, key):
    """Adds a key to a node in hash_ring"""
    key_hash = self._hash(key)
    for node_hash, _ in self.ring:
      if(key_hash > node_hash):
        self.keys[node_hash].append(key_hash)
        return
    self.keys[self.ring[0][0]].append(key_hash)

  
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
    index = bisect(self.ring, (key_hash, ))
    return self.ring[index % len(self.ring)][1]
  
  def get_key(self, node):
      """Returns the keys associated with a given virtual node."""
      node_hash = self._hash(node)
      return self.keys.get(node_hash, [])

ring = HashRing()
ring._init_([], 4)
ring.add_node('node 1')
ring.add_key('something')
ring.add_key('absabdaosudas')
ring.add_key('sdbfnsdfo')
ring.add_key('iwyergifbs')
ring.add_node('node 2')
ring.add_key('asasdasd')
ring.add_key('udbaidwe')
ring.add_key('jdfiwdiw')
ring.add_node('node 3')
print(ring.keys)