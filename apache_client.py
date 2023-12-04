from csv import DictReader
import time
import pyarrow.flight as flight
import pyarrow as pa

class ApacheClient:
    def __init__(self, host = 'localhost', port = 8815):
        self.location = flight.Location.for_grpc_tcp(host, port)
        self.connection = flight.connect(self.location)
        self.connection.wait_for_available()
    
    def put_table(self, name, table):
        table_name = name.encode('utf8')
        descriptor = flight.FlightDescriptor.for_command(table_name)
        writer, reader = self.connection.do_put(descriptor, table.schema)
        writer.write(table)
        writer.close()
    
    def get_table(self, name):
        table_name = name.encode('utf8')
        ticket = flight.Ticket(table_name)
        reader = self.connection.do_get(ticket)
        return reader.read_all()


client = ApacheClient()

with open('./companies_sorted.csv', mode ='r') as file:   
        dict_reader = DictReader(file)
     
        list_of_dict = list(dict_reader)
    
        #print(list_of_dict)
count =0
for rec in list_of_dict:
    #count= count+1
    table =  pa.Table.from_pylist([rec])
    #if count ==3:
    client.put_table(rec["id"],table)
    #time.sleep(20)

#client.get_table("5944912")