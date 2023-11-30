import pathlib
import pyarrow as pa
import pyarrow.flight as fl
import pandas as pd

# Define a Flight endpoint
location = fl.Location.for_grpc_tcp('localhost', 5000)

# Define a FlightInfo object with schema and data
schema = pa.schema()

data = [
    pa.array([1, 2, 3]),
    pa.array(['Alice', 'Bob', 'Charlie'])
]

batch = pa.record_batch(data, schema)
flight_info = fl.FlightInfo(schema)

# Define a Flight endpoint to serve the FlightInfo and RecordBatch
class FlightServer(fl.FlightServerBase):
    def __init__(self, location="grpc://0.0.0.0:8815", repo=pathlib.Path("./datasets"), **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        self._location = location
        self._repo = repo
        self._data_store = {}
        
    def list_flights(self, context, criteria):
        return [fl.FlightEndpoint("example", [flight_info])]

    def do_get(self, context, ticket):
        return fl.RecordBatchStream(batch)
    
    def health_check(self, context, ticket):
        if ticket == b"health-check":
            # Respond to health check requests
            print("Server-1")
            return fl.RecordBatchStream(pa.RecordBatch.from_pandas(pd.DataFrame({'response': ['healthy']})))
        
    def store_key(self, context, descriptor, reader, writer):
        # Read the incoming data from the gateway
        batch = reader.read_next_batch()

        # Store the data in memory using a unique key
        key = f"{descriptor.path}_{context.peer()}"
        self._data_store[key] = batch

        # Respond to the gateway
        schema = pa.schema([('response', pa.string())])
        response_data = pa.array(['Data stored successfully'], type=pa.string())
        response_batch = pa.RecordBatch.from_pandas(pd.DataFrame({'response': response_data}))
        writer.write_batch(response_batch)
        writer.close()

server = fl.FlightServer()
server.start(location)
print("Starting server2...")
