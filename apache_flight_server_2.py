import pyarrow as pa
import pyarrow.flight as fl

# Define a class (for demonstration purposes)
class MyClass:
    def __init__(self, data):
        self.data = data

# Define a list to store objects
object_list = []

# Define a Flight service that receives a class object and stores it in the list
class MyFlightServer(fl.FlightServerBase):
    def __init__(self, host='127.0.0.1', port='5002'):
        super().__init__()
        self.location = f'grpc://{host}:{port}'
    
    def do_put(self, context, descriptor, reader, writer):
        # Deserialize the object from Flight stream
        data = reader.read()
        deserialized_object = pa.deserialize(data)
        
        # Check if the received object is an instance of MyClass and store it
        if isinstance(deserialized_object, MyClass):
            object_list.append(deserialized_object)
            writer.write(b"Object stored successfully!")
        else:
            writer.write(b"Invalid object type!")

# Set the server location
#location = fl.Location.for_grpc_tcp('localhost', 8080)

# Start the Flight server
server = MyFlightServer()
print(server.location)
server.serve()