import pyarrow as pa
import pyarrow.flight as flight
from hash_ring import HashRing
import threading

#to send the data to respective servers
class GatewayClient:
    def __init__(self, port, host = 'localhost'):
        self.location = flight.Location.for_grpc_tcp(host, port)
        self.connection = flight.connect(self.location)
        self.connection.wait_for_available()
    
    #will connect to the server and send the table with the key
    def put_table(self, name, table):
        table_name = name
        descriptor = flight.FlightDescriptor.for_command(table_name)
        writer, reader = self.connection.do_put(descriptor, table.schema)
        writer.write(table)
        writer.close()
    
    #will connect to the server and get the table for the key asked by the client
    def get_table(self, name):
        table_name = name
        ticket = flight.Ticket(table_name)
        reader1 = self.connection.do_get(ticket)
        #print(type(reader1))
        return reader1


#to act as the server for clients
class Gateway(flight.FlightServerBase):
  def __init__(self, location, server_locations=set()):
    super(Gateway, self).__init__(location)
    self.server_locations = server_locations
    self.hr = HashRing(data_replication =2)
    thread = threading.Thread(target=self.run_health_check())
    thread.start()

  #sends data to server by calculating the virtual node location      
  def do_put(self, context, descriptor, reader, writer):
    #get data from apache client
    table_name = descriptor.command
    table = reader.read_all()
    # print("table:")
    # print(table)

    # Determine the servers to forward the data along with replicated hashes
    target_servers = self.hr.add_key(table_name.decode('utf8'))

    for server in target_servers:
      client = GatewayClient(int(server.split(':')[-1]))
      
      #send to server
      thread1 = threading.Thread(target=client.put_table(table_name, table))
      thread1.start()
      thread1.join()
    #self.hr.print_data()
    self.hr.print_summary()


  # fetches the data requested by client using the key value
  def do_get(self, context, ticket):
    table_name = ticket.ticket
    key = table_name.decode('utf-8')

    target_server = self.hr.get_node(key)
    client = GatewayClient(int(target_server.split(':')[-1]))
    
    #fetch from server
    reader1 = client.get_table(table_name)

    return flight.RecordBatchStream(reader1.read_all())

  def add_server(self, server):
    self.hr.add_node(server)
        
  def remove_server(self, server):
    self.hr.remove_node(server)

  def health_check(self, server):
    try:
      action = flight.Action('health_check', b'')
      client = flight.FlightClient(server)
      results = client.do_action(action)
      for result in results:
        if server not in self.hr.nodes:
          self.add_server(server)
          #self.hr.print_data()
          self.hr.print_summary()

        print(f"Server: {server} is healthy")
        return
      print(f"Health check for {server} passed, but server didn't respond as expected")
      return
    except Exception as e:
      if server in self.hr.nodes:
        self.remove_server(server)
        #self.hr.print_data()
        self.hr.print_summary()
        
        print(f"Health check failed for server: {server}")
        
      return

  def run_health_check(self,):
    threading.Timer(10.0, self.run_health_check).start()
    for server in self.server_locations:
      self.health_check(server)

if __name__ == "__main__":
  # Server locations
  servers = ["grpc://localhost:8816", "grpc://localhost:8817", "grpc://localhost:8818"]

  # Start the gateway server
  gateway = Gateway("grpc://localhost:8815", servers)
  print("Starting the gateway...")
  gateway.serve()