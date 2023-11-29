import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import csv
from company import Company
import pickle


class Client:
    def __init__(self, location):
        self.client = flight.FlightClient(location)
        self.schema = pa.schema([('response', pa.string())])

    def add_key(self, object):
        #serialize the python class object
        serialized_object = pickle.dumps(object)

        #arrow array with serialized object
        data = pa.array([serialized_object], type=pa.string())
        '''
        # Extract attributes from the object
        attributes = vars(sample_person)

        # Create Arrow fields based on the attributes
        arrow_fields = [
            pa.field(attr_name, pa.string()) if isinstance(attr_value, str) else pa.field(attr_name, pa.int64())
            for attr_name, attr_value in attributes.items()
        ]

        # Create the Arrow schema
        arrow_schema = pa.schema(arrow_fields)
        '''

        #schema for the RecordBatch
        schema = pa.schema([('id', pa.int64()), ('name', pa.string()), ('domain', pa.string()), ('year_founded', pa.int64()), 
                            ('industry', pa.string()), ('size_range', pa.int64()), ('locality', pa.string()), ('country', pa.string()), 
                            ('linkedin_url', pa.string()), ('current_employee_estimate', pa.int64()), 
                            ('total_employee_estimate', pa.int64())])

        # Create the RecordBatch
        record_batch = pa.RecordBatch.from_arrays([data], schema)
        
        # Send the RecordBatch to the gateway
        writer, _ = self.client.do_put(
            flight.FlightDescriptor.for_path("string-push"), schema)
        writer.write_batch(record_batch)
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
    client = Client("grpc://localhost:1000")

    #send data from csv files
    with open('../companies_sorted.csv', mode ='r') as file:   
        reader = csv.reader(file)
        for row in reader:
            obj = Company(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
            client.add_data(obj)

    # Example of receiving data
    received_data = client.get_node("Some key here")
    print("Received Data:", received_data)
