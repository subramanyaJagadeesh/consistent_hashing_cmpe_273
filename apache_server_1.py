import pathlib
import pyarrow as pa
import pyarrow.flight as fl
import pandas as pd
import pickle
from company import Company

# Define a Flight endpoint to serve the FlightInfo and RecordBatch
class FlightServer(fl.FlightServerBase):
    def __init__(self, location, repo=pathlib.Path("./datasets"), **kwargs):
        super(FlightServer, self).__init__(location)
        self._location = location
        self._repo = repo
        self.tables = {}
        self._data_store = {}
    
    #Mainly used for checking server health
    def do_action(self, context, action):
        if action.type == 'health_check':
            # Perform the health check
            yield fl.Result(b'Server is healthy')
        else:
            raise fl.FlightUnimplementedError(f'Unknown action: {action.type}')
        
          
    def do_put(self, context, descriptor, reader, 
               writer):
        table_name = descriptor.command
        print("Table_name: ")
        print(table_name)
        self.tables[table_name] = reader.read_all()
        # print(1)
        # print(len(self.tables[table_name]))
        # print(2)
        # print(self.tables)
        # print(3)
        # print(self.tables[table_name])
    
    def do_get(self, context, ticket):
        table_name = ticket.ticket
        print("recieved:")
        print(table_name)
        table = self.tables[table_name]
        data = fl.RecordBatchStream(table)
        print(type(data))
        return data


    '''
    #Used to get the key from data store
    def do_get(self, context, ticket):
        key = ticket.ticket.decode('utf-8')
        py_obj = self._data_store[key]
        serialized_obj = pickle.dumps(py_obj)
        # Create a PyArrow Table with a single binary column
        data = pa.Table.from_arrays([pa.array([serialized_obj], type=pa.binary())], names=['data'])
        return fl.RecordBatchStream(data)
    
    #Used to store the key in data store
    def do_put(self, context, descriptor, reader, writer):
        table = reader.read_all()
        # Extract the binary data (assuming it's in a column named 'data')
        serialized_obj = table.column('data').to_pylist()[0]
        # Deserialize the Python object
        py_obj = pickle.loads(serialized_obj)

        # Store the data in memory using a unique key
        self._data_store[py_obj['id']] = py_obj

        # Respond to the gateway
        response_data = pa.array(['Data stored successfully'], type=pa.string())
        response_batch = pa.RecordBatch.from_pandas(pd.DataFrame({'response': response_data}))
        writer.write_batch(response_batch)
        writer.close()
    '''
server = FlightServer("grpc://localhost:8816")
print("Starting server 1 at 8816...")

server.serve()
