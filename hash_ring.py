import hashlib
from bisect import bisect, insort
import mmh3

class LinkedNode:
  def __init__(self, key):
    self.key = key
    self.next = None

class HashRing:
  def __init__(self, nodes=None, virtual_nodes=3, data_replication =0):
    # Number of virtual nodes for each physical node
    self.virtual_nodes = virtual_nodes

    #Number of time data is replicated
    self.data_replication = data_replication

    #Physical nodes
    self.nodes = set()
    #Hash ring
    self.ring = []
    #To keep a log of hash_keys stored in a particular virtual node
    self.keys = {}

    #node to node_hash
    self.node_map = {}

    if nodes:
      for node in nodes:
        self.add_node(node)
        self.keys[self.hash_function(node)] = {}
  
  def hash_function(self, key, seed=0):
    "Returns hash for replication"
    #The bitwise & 0xFFFFFFFF operation is applied to the hash value to make it positive
    #return mmh3.hash128(key) & 0xFFFFFFFF 
    return mmh3.hash(key, seed) & 0xFFFFFFFF 

  def add_node(self, node):
    """Adds a node to the hash ring with its replicas."""
    for i in range(self.virtual_nodes):
      replica_key = f"{node}-{i}"
      node_hash = self.hash_function(replica_key)
      self.keys[node_hash] = {}
      self.node_map[node_hash] = node
      self.nodes.add(node)
      insort(self.ring, node_hash)
      node_index = bisect(self.ring, node_hash)

      if node_index == len(self.ring):
        node_index = 0
      keys_to_rehash = self.keys.get(self.ring[node_index], {})
      if keys_to_rehash:
        self.keys[self.ring[node_index]] = {}

        for key, linkedNode in keys_to_rehash.items():
          self.add_key(key, linkedNode.key)
  
  def add_key(self, key, existing_hash=-1):
    """Adds a key to a node in hash_ring"""
    target_servers = []
    key_hash = ''
    if existing_hash ==-1:
      key_hash = self.hash_function(key)
    else:
      key_hash = existing_hash
    
    #add replicated key hashes
    replicated_hashes = self.replicate(key)
    
    #this is for actual key_hash
    found_pos = -1
    for i in range(1, len(self.ring)):
      prev = self.ring[i-1]
      curr = self.ring[i]
      #insert hash and replicas in node matched
      if(key_hash > prev and key_hash < curr):
        found_pos = i
        self.add_list_node(key_hash, key, self.ring[found_pos])
        break

    if found_pos == -1:
      found_pos = 0
      #insert hash in first node since no node matched the criteria
      self.add_list_node(key_hash, key, self.ring[found_pos])

    #to send back the node server that stores the key_hash
    target_servers.append(self.node_map[self.ring[found_pos]])
    #this is for replica hashes of key_hash
    for replica in replicated_hashes:
      found_pos = -1
      for i in range(1, len(self.ring)):
        prev = self.ring[i-1]
        curr = self.ring[i]
        #insert hash and replicas in node matched
        if(replica > prev and replica < curr):
          found_pos = i
          self.add_list_node(replica, key, self.ring[found_pos])
          break
      if found_pos == -1:
        found_pos = 0
        #insert hash in first node since no node matched the criteria
        self.add_list_node(replica, key, self.ring[found_pos])
      target_servers.append(self.node_map[self.ring[found_pos]])
    return target_servers

  def add_list_node(self, key_hash, key, curr):
    if key not in self.keys[curr]:
      head = LinkedNode(key_hash)
      self.keys[curr][key] = head
    else:
      head = self.keys[curr][key]
      while head.next is not None:
        head = head.next
      head.next = LinkedNode(key_hash)
  
  def remove_node(self, node):
    """Removes a node and its replicas from the hash ring."""
    for i in range(self.virtual_nodes):
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

      #remove the node map for the virtual node
      del(self.node_map[node_hash])

      #rehash the keys and add them to 
      for key, linkedNode in keys_to_rehash.items():
        self.add_key(key, linkedNode.key)
    self.nodes.remove(node)

  def remove_key(self, key):
    """Removes a key the virtual node in hash ring."""
    original_hash = self.hash_function(key)
    replicated_hashes = self.replicate(key)

    replicated_hashes.insert(0, original_hash)

    server_nodes = []
    for key_hash in replicated_hashes:
      node_index = bisect(self.ring, key_hash)

      if node_index == len(self.ring):
        node_index = 0
      #to send back the node server that stores the key_hash
      server_nodes.append(self.node_map[self.ring[node_index]])

      #search for listnodes in case of collision
      currentNode = self.keys.get(node_index)[key_hash]
      prev = None
      while currentNode is not None:
        if currentNode.key == key:
          #removeNode(self, prev, currentNode, node_hash, key_hash)
          
          if prev == None:
            if currentNode.next == None:
              del self.keys.get(node_index)[key_hash]
            else:
              self.keys.get(node_index)[key_hash] = currentNode.next
          else:
            prev.next = currentNode.next
          
          return
        prev = current
        current = current.next
    return server_nodes

  def remove_list_node(self, prev, currentNode, node_hash, key_hash):
    if prev == None:
      if currentNode.next == None:
        del self.keys.get(node_hash)[key_hash]
      else:
        self.keys.get(node_hash)[key_hash] = currentNode.next
    else:
      prev.next = currentNode.next
    return

  def get_node(self, key, isHashedAlready=False):
    """Returns the node to which the given key is mapped."""
    key_hash = self.hash_function(key) if not isHashedAlready else key

    # Find the position in the ring where the key_hash belongs
    for i in range(1, len(self.ring)):
        prev = self.ring[i - 1]
        curr = self.ring[i]
        if key_hash > prev and key_hash < curr:
            return self.node_map[curr]

    # If the key_hash doesn't belong to any existing position, return the first node
    return self.node_map[self.ring[0]]
  

  def replicate(self, key):
    replicated_hash_keys =[]
    for i in range(0, self.data_replication):
      replicated_hash_keys.append(self.hash_function(key, 12345+i))
    return replicated_hash_keys
  
  def print_data(self):
      print("===========================================================================================================================================")
      print("Current configuration:")
      print("Rings: ")
      print(self.ring)
      print("-------------------------------------------------------------------------------------------------------------------------------------------")
      print("Node map")
      print(self.node_map)
      print("-------------------------------------------------------------------------------------------------------------------------------------------")
      
      for virtual_node, value in self.keys.items():
        print("Virtual Node: " + str(virtual_node))
        for key, link_node in value.items():
          print("       Key: " + str(key))
          print("       Data: ")
          print("              "+ str(link_node.key))
          while link_node.next!= None :
            link_node = link_node.next
            print("              "+ str(link_node.key))
          print()
      print("===========================================================================================================================================")
      print()