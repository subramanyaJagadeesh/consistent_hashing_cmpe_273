import pyarrow as pa
import pyarrow.flight as fl
from company import Company

# Define a class to be sent
class MyClass:
    def __init__(self, data):
        self.data = data

# Define a Flight server class
class FlightServer(fl.FlightServerBase):
    def __init__(self, host='localhost', port=8080):
        super().__init__()
        self.location = f'grpc+tcp://{host}:{port}'

    def list_actions(self, context):
        return [fl.ActionType(action_type="send_data_obj", description="Send Company object")]

    def do_action(self, context, action):
        if action.type == "send_data_obj":
            # Create an instance of MyClass and serialize it
            obj = MyClass("Hello from MyClass!")
            serialized_obj = pa.serialize(obj).to_buffer()
            # Return the serialized object as FlightPayload
            return fl.Result(pyload=fl.FlightPayload(buffer=serialized_obj))

# Start the server
server = MyFlightServer()
server.start()
print(f"Flight server listening on {server.location}")

# Keep the server running until interrupted
try:
    server.wait()
except KeyboardInterrupt:
    server.shutdown()
