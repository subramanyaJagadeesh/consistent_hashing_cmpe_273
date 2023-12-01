import pyarrow as pa
import pyarrow.flight as fl

# Define a class (for demonstration purposes)
class MyClass:
    def __init__(self, data):
        self.data = data

# Data to be sent to the server
obj_to_send = MyClass("Example data to send")

# Convert the object attributes into a dictionary
data_dict = obj_to_send.__dict__

# Convert the dictionary to a PyArrow Table
table = pa.Table.from_pydict({key: [value] for key, value in data_dict.items()})

# Convert the table to a RecordBatch for serialization
record_batch = pa.RecordBatch.from_pandas(table.to_pandas())

# Serialize the RecordBatch to send
serialized_record_batch = record_batch.serialize()
# Connect to the Flight server
client = fl.connect('grpc://127.0.0.1:8080', disable_server_verification = True)

# Get a Flight endpoint for putting data
endpoint = fl.FlightEndpoint(b'', [])

# Create a Flight descriptor
flight_info = pa.schema(table.schema)
flight_desc = fl.FlightDescriptor.for_path('utf-8')
#client.get_flight_info(flight_desc)

# Get a Flight writer to send data to the server
writer, metadata_reader = client.do_put(flight_desc, flight_info)

# Write the Flight payload (serialized object)
writer.write(serialized_record_batch.to_buffer())
writer.close()
# Get the server response
result = writer.get_result()
print(result)

'''
import pickle
import pyarrow as pa
import pyarrow.flight as fl
from pyarrow.flight import FlightStreamWriter

# Define a class (for demonstration purposes)
class MyClass:
    def __init__(self, data):
        self.data = data

# Data to be sent to the server
obj_to_send = MyClass("Example data to send")

# Serialize the object to send
serialized_object = pickle.dumps(obj_to_send)

# Connect to the Flight server
client = fl.connect('grpc://localhost:8080')
# Create a Flight descriptor
schema = pa.schema([("data", pa.string())])
flight_info = pa.flight.FlightDescriptor.for_path(serialized_object)
endpoint = fl.FlightEndpoint(b'', [serialized_object])

writer = FlightStreamWriter()

# Get a Flight writer to send data to the server
client.do_put(endpoint, schema)

# Write the Flight payload (serialized object)
writer.write(serialized_object)
writer.close()

# Get the server response
result = writer.get_result()
print(result)
'''