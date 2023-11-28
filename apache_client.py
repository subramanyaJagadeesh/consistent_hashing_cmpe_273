import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd

class Client:
    def __init__(self, location):
        self.client = flight.FlightClient(location)
        self.schema = pa.schema([('response', pa.string())])

    def add_key(self, string_data):
        # Send a single string to the gateway
        data = pa.array([string_data], type=pa.string())
        schema = pa.schema([('string', pa.string())])
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

        # Extract the string from the RecordBatch
        node = record_batch.column(0).to_pylist()[0]  # Assuming a single string
        print(f"{key} is present in Node: {node}")

if __name__ == "__main__":
    # Connect to the gateway (replace with actual gateway address)
    client = Client("grpc://localhost:1000")

    # Example of sending data
    client.add_key("Some key here")

    # Example of receiving data
    received_data = client.get_node("Some key here")
    print("Received Data:", received_data)
