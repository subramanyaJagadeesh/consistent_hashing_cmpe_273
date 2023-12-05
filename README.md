## CMPE 273 Final Project
## Topic: Consistent Hashing with Apache Arrow Flight
## Team: Lords of the Hashrings  
## Team members:
* Subramanya Jagadeesh (016218293)
* Sayali Vinod Bayaskar (017455451)
* Vineet Jaywant (017408638)

## Contributions:
* Subramanya Jagadeesh
  - Client, Gateway, and Server Architecture Solution
  - hashring.py - add_node, add_key, add_list_node, __init__
  - apache_gateway.py - GatewayServer’s __init__ , add_server, remove_server, health_check, run_health_check
  - apache_server.py - __init__, do_action(for health check reply)
* Sayali Bayaskar
  - Data replication and Collision control
  - hash_ring.py - remove_node, remove_key, remove_list_node, replicate, print data
  - apache_gateway.py - Gateway_client class, GatewayServer’s do_put
  - apache_server.py - do_put
  - apache_client.py
* Vineet Jaywant
  - Architecture diagram for the project
  - hash_ring.py - get_node
  - apache_gateway - do_get
  - apache_server.py - do_get
