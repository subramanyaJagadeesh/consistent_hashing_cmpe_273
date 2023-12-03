import pyarrow.flight as flight

class TinyServer(flight.FlightServerBase):
  
    def __init__(self, 
                 host = 'localhost', 
                 port = 8080):
        self.tables = {}
        self.location = flight.Location.for_grpc_tcp(host, port)
        super().__init__(self.location)
      
    def do_put(self, context, descriptor, reader, 
               writer):
        table_name = descriptor.command
        print("Table_name: ")
        print(table_name)
        self.tables[table_name] = reader.read_all()
        print(1)
        print(len(self.tables[table_name]))
        print(2)
        print(self.tables)
        print(3)
        print(self.tables[table_name])
    
    def do_get(self, context, ticket):
        table_name = ticket.ticket
        table = self.tables[table_name]
        return flight.RecordBatchStream(table)

server = TinyServer()
print("Starting server1 at 8080...")

server.serve()