import pyarrow as pa
import pyarrow.flight as flight
from hash_ring import HashRing
import threading
import pickle

class Gateway(flight.FlightServerBase):
  def __init__(self, location, server_locations=set()):
    super().__init__(location)
    self.server_locations = server_locations
    self.hr = HashRing()

  def do_put(self, context, descriptor, reader, writer):

    # Read the incoming data from the client
    batch = reader.read_next_batch()
    serialized_object = batch.column(0).to_pybytes()
    
    #serialized_object = reader.read().to_pybytes()

    # Deserialize the object using pickle
    company_object = pickle.loads(serialized_object)
    print("Received Person object on server:", company_object.name)

    # Determine the server to forward the data
    target_server = self.hr.add_key(company_object.id)

    # Forward the data to the chosen server
    client = flight.FlightClient(target_server)
    writer, _ = client.store_key(
        flight.FlightDescriptor.for_path("string-push"), batch.schema)
    writer.write_batch(batch)
    writer.close()

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
    node = self.hr.get_node(ticket)
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
    client = flight.FlightClient(server)
    reader = client.health_check(flight.Ticket(b"health-check"))
    table = reader.read_all()
    if not table:
      self.remove_server(server)
      return
    if server not in self.hr.nodes:
      self.add_server(server)

  def run_health_check(self,):
    threading.Timer(1.0, self.run_health_check).start()
    for server in servers:
      self.health_check(server)
  

if __name__ == "__main__":
  # Server locations (replace with actual server addresses)
  servers = ["grpc://localhost:2000", "grpc://localhost:3000", "grpc://localhost:4000"]

  # Start the gateway server
  gateway = Gateway("grpc://localhost:1000", servers)
  print("Starting the gateway...")
  gateway.serve()
  gateway.run_health_check()