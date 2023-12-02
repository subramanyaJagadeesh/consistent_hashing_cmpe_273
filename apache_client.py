import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import csv
from company import Company
import pickle


class Client:
    def __init__(self, location):
        self.client = flight.FlightClient(location)

    def add_key(self, key, object):
        #serialize the python class object
        serialized_obj = pickle.dumps(object)

        #schema for the RecordBatch
        # schema = pa.schema([('id', pa.int64()), ('name', pa.string()), ('domain', pa.string()), ('year_founded', pa.int64()), 
        #                     ('industry', pa.string()), ('size_range', pa.int64()), ('locality', pa.string()), ('country', pa.string()), 
        #                     ('linkedin_url', pa.string()), ('current_employee_estimate', pa.int64()), 
        #                     ('total_employee_estimate', pa.int64())])

        # Create the RecordBatch
        # record_batch = pa.RecordBatch.from_arrays([data], schema)
        table = pa.Table.from_arrays([pa.array([serialized_obj], type=pa.binary())], names=['data'])
        descriptor = flight.FlightDescriptor.for_path(key)
        writer, _ = self.client.do_put(descriptor, table.schema)
        writer.write_table(table)
        writer.close()
        
    def get_node(self, key):
        # Receive data (string) from the gateway
        reader = self.client.do_get(flight.Ticket(f"{key}"))
        record_batch = reader.read_next_batch()
        '''
        # Extract the serialized string from the RecordBatch column
        serialized_object = record_batch.column(0)[0].as_py()

        # Deserialize the Python class object from the serialized string
        company_object = pickle.loads(serialized_object)

        # deserialized Python object
        print(f"The object for {key} is : {company_object}")
        '''

        # Extract the string from the RecordBatch
        node = record_batch.column(0).to_pylist()[0]  # Assuming a single string
        print(f"{key} is present in Node: {node}")

if __name__ == "__main__":
    # Connect to the gateway (replace with actual gateway address)
    client = Client("grpc://localhost:8815")

    #send data from csv files
    with open('./companies_sorted.csv', mode ='r') as file:   
        reader = csv.reader(file)
        for row in reader:
            if row[0] == 'id':
                continue
            obj = Company(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
            client.add_key(row[0], obj)

    # Example of receiving data
    received_data = client.get_node("Some key here")
    print("Received Data:", received_data)
