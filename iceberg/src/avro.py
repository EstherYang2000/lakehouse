import fastavro

# Define the path to your Avro file
avro_file = 'data/iceberg.db/namelist_dataset/metadata/snap-6692502368035178363-0-220afb16-2105-4d35-8f97-7294e38d5215.avro'

# Open the Avro file in read mode
with open(avro_file, 'rb') as f:
    # Parse the Avro file
    reader = fastavro.reader(f)
    
    # Iterate over records in the Avro file
    for record in reader:
        print(record)
