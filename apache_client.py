import pyarrow.flight as fl

# Connect to the Flight server
client = fl.FlightClient.connect('grpc://localhost:5000')

# List available flights (just one in this example)
flights = client.list_flights()
for flight in flights:
    print(f"Flight: {flight}")

# Get the schema and data
flight_info = flights[0]
schema = flight_info.schema
reader = client.do_get(flight_info.endpoints[0].ticket)
batches = [b for b in reader]
table = batches[0].to_table()

# Display the retrieved table
print("\nRetrieved Table:")
print(table)
