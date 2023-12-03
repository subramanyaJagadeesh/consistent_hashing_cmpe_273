import pyarrow as pa
import pyarrow.flight as flight
from hash_ring import HashRing
import threading
import pickle

class Gateway(flight.FlightServerBase):
  def __init__(self, location, server_locations=set()):
    super(Gateway, self).__init__(location)
    self.server_locations = server_locations
    self.hr = HashRing()
    thread = threading.Thread(target=self.run_health_check())
    thread.start()

  def do_put(self, context, descriptor, reader, writer):
    # Read the incoming data from the client
    table = reader.read_all()
    serialized_obj = table.column('data').to_pylist()[0]
    
    #serialized_object = reader.read().to_pybytes()

    # Deserialize the object using pickle
    company_object = pickle.loads(serialized_obj)
    print("Received Person object on server:", company_object.name)

    # Determine the server to forward the data
    target_server = self.hr.add_key(company_object.id)

    # Forward the data to the chosen server
    client = flight.FlightClient(target_server)
    table = pa.Table.from_arrays([pa.array([serialized_obj], type=pa.binary())], names=['data'])
    descriptor = flight.FlightDescriptor.for_path(company_object.id)
    writer, _ = client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.close()
    return flight.Result(b'Server is healthy')

  '''
  def do_put(self, context, descriptor, reader, writer):
    # Read the incoming data from the client
    batch = reader.read_next_batch()
    string_data = batch.column(0).to_pylist()[0]

    # Determine the server to forward the data
    target_server = self.hr.add_key(string_data)

    # Forward the data to the chosen server
    client = flight.FlightClient(target_server)
    writer, _ = client.store_key(
        flight.FlightDescriptor.for_path("string-push"), batch.schema)
    writer.write_batch(batch)
    writer.close()
  '''
  def do_get(self, context, ticket):
    key = ticket.ticket.decode('utf-8')
    node = self.hr.get_node(key)
    data = pa.array([node], type=pa.string())
    schema = pa.schema([('response', pa.string())])
    record_batch = pa.RecordBatch.from_arrays([data], schema)

    # Stream the RecordBatch back to the client
    return flight.RecordBatchStream([schema], [record_batch])
    
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
        print(f"Server: {server} is healthy")
        return
      print(f"Health check for {server} passed, but server didn't respond as expected")
      return
    except Exception as e:
      self.remove_server(server)
      print(f"Health check failed for server: {server} with error: {e}")
      return

  def run_health_check(self,):
    threading.Timer(1.0, self.run_health_check).start()
    for server in self.server_locations:
      self.health_check(server)

if __name__ == "__main__":
  # Server locations (replace with actual server addresses)
  servers = ["grpc://localhost:8816", "grpc://localhost:8817", "grpc://localhost:8818"]

  # Start the gateway server
  gateway = Gateway("grpc://localhost:8815", servers)
  print("Starting the gateway...")
  gateway.serve()