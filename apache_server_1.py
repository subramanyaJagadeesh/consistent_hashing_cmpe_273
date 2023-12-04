import pathlib
import pyarrow.flight as fl

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
        
          
    #store data in local dictionary
    def do_put(self, context, descriptor, reader, 
               writer):
        table_name = descriptor.command
        self.tables[table_name] = reader.read_all()
        print("Keys:")
        print(self.tables.keys())
    
    #retrive data from dictionary and send the batch stream to gateway
    def do_get(self, context, ticket):
        table_name = ticket.ticket
        print("Request recieved for " + str(table_name))
        table = self.tables[table_name]
        data = fl.RecordBatchStream(table)
        print(type(data))
        return data


server = FlightServer("grpc://localhost:8816")
print("Starting server 1 at 8816...")

server.serve()
