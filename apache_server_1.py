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
        # print("Table_name: ")
        # print(table_name)
        self.tables[table_name] = reader.read_all()
        # print(1)
        # print(len(self.tables[table_name]))
        # print(2)
        # print(self.tables)
        # print(3)
        # print(self.tables[table_name])
        print("Keys:")
        print(self.tables.keys())
    
    def do_get(self, context, ticket):
        table_name = ticket.ticket
        print("recieved:")
        print(table_name)
        table = self.tables[table_name]
        data = fl.RecordBatchStream(table)
        print(type(data))
        return data


server = FlightServer("grpc://localhost:8816")
print("Starting server 1 at 8816...")

server.serve()
