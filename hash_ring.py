import hashlib
from bisect import bisect, insort
import mmh3

class LinkedNode:
    def __init__(self, key):
        self.key = key
        self.next = None

class HashRing:
  def __init__(self, nodes=None, num_replicas=2, data_replication =0):
    # Number of virtual nodes for each physical node
    self.num_replicas = num_replicas

    #Number of time data is replicated
    self.data_replication = data_replication

    #Physical nodes
    self.nodes = set()
    #Hash ring
    self.ring = []
    #To keep a log of hash_keys stored in a particular virtual node
    self.keys = {}

    if nodes:
      for node in nodes:
        self.add_node(node)
        self.keys[self.hash_function(node)] = {}

  def _hash(self, key):
    """Returns a hash for the given key."""
    return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)%100000
  
  def hash_function(self, key, seed=0):
    "Returns hash for replication"
    #return mmh3.hash128(key)
    #The bitwise & 0xFFFFFFFF operation is applied to the hash value to make it positive
    return mmh3.hash(key, seed) & 0xFFFFFFFF 

  def add_node(self, node):
    """Adds a node to the hash ring with its replicas."""
    for i in range(self.num_replicas):
      replica_key = f"{node}-{i}"
      node_hash = self.hash_function(replica_key)
      self.keys[node_hash] = {}
      self.nodes.add(node)
      insort(self.ring, node_hash)
      node_index = bisect(self.ring, node_hash)

      if node_index == len(self.ring):
        node_index = 0
      keys_to_rehash = self.keys.get(self.ring[node_index], {})
      if keys_to_rehash:
        self.keys[self.ring[node_index]] = {}

        for key_hash, key in keys_to_rehash.items():
          self.add_key(key, key_hash)
  
  def add_key(self, key, existing_hash=-1):
    """Adds a key to a node in hash_ring"""
    key_hash = ''
    if existing_hash ==-1:
      key_hash = self.hash_function(key, self.data_replication)
    else:
      key_hash = existing_hash
    
    #add replicated key hashes
    replicated_hashes = self.replicate(key)
    
    for i in range(1, len(self.ring)):
      prev = self.ring[i-1]
      curr = self.ring[i]
      if(key_hash > prev and key_hash < curr ):

        #implement logic for LinkedList for duplicate hash keys 

        self.keys[curr][key_hash] = LinkedNode (key)
        return
    self.keys[self.ring[0]][key_hash] = LinkedNode (key)

  
  def remove_node(self, node):
    """Removes a node and its replicas from the hash ring."""
    
    for i in range(self.num_replicas):
      #get virtual node
      replica_key = f"{node}-{i}"

      #get hash of virtual node
      node_hash = self.hash_function(replica_key)
      
      #get index of virtual node in the hashing ring
      ind = self.ring.index(node_hash)

      #get list of keys which will be rehashed after removing this virtual node
      keys_to_rehash = self.keys.get(self.ring[ind], {})

      #remove the keys of the virtual node of the "to be rehased" keys from the key list 
      
      del self.keys[self.ring[ind]]

      #remove the virtual node
      self.ring.remove(node_hash)

      #rehash the keys and add them to 
      for key_hash, key in keys_to_rehash.items():
          self.add_key(key, key_hash)
    self.nodes.remove(node)

  def remove_key(self, key):
    """Removes a key the virtual node in hash ring."""
    key_hash = self.hash_function(key, self.data_replication)
    print(key_hash)
    for node_hash in self.ring:
      print(self.keys[node_hash])
      if(key_hash < node_hash):
        currentNode = self.keys.get(node_hash)[key_hash]
        prev = None
        while currentNode is not None:
          if currentNode.key == key:
            #removeNode(self, prev, currentNode, node_hash, key_hash)
            
            if prev == None:
              if currentNode.next == None:
                del self.keys.get(node_hash)[key_hash]
              else:
                self.keys.get(node_hash)[key_hash] = currentNode.next
            else:
              prev.next = currentNode.next
            
            return
          prev = current
          current = current.next

  def removeNode(self, prev, currentNode, node_hash, key_hash):
    if prev == None:
      if currentNode.next == None:
        del self.keys.get(node_hash)[key_hash]
      else:
        self.keys.get(node_hash)[key_hash] = currentNode.next
    else:
      prev.next = currentNode.next
    return

  def get_node(self, key):
    """Returns the node to which the given key is mapped."""
    if not self.ring:
        return None
    key_hash = self.hash_function(key)
    index = bisect(self.ring, key_hash)
    return self.ring[index % len(self.ring)][1]
  
  def get_key(self, node):
      """Returns the keys associated with a given virtual node."""
      node_hash = self.hash_function(node)
      return self.keys.get(node_hash, [])

  def replicate(self, key):
    replicated_hash_keys =[]
    for i in range(0, self.data_replication):
      replicated_hash_keys.append(self.hash_function(key, 12345+i))
    return replicated_hash_keys

def main():
  hashRing = HashRing()
  hashRing.add_node('A')
  hashRing.add_node('B')

  print(hashRing.ring)

  hashRing.add_key(key = "Apache")
  hashRing.add_key(key = "Arrow")
  hashRing.add_key(key = "B-7")

  print(hashRing.keys)
  
  hashRing.add_node('C')
  print(hashRing.ring)
  print(hashRing.keys)

  hashRing.add_key(key = "Consistent")
  hashRing.add_key(key = "Hashing")
  hashRing.add_key(key = "Algorithm")

  print(hashRing.keys)

  hashRing.remove_key("Arrow")
  
  print(hashRing.keys)

  hashRing.remove_node('A')
  print(hashRing.ring)
  print(hashRing.keys)

  
if __name__=="__main__": 
    main()
