import sqlite3
import json
import hashlib

# Open the database connection
conn = sqlite3.connect('/home/vboxuser/Downloads/RDS_2023.06.1_modern_minimal/RDS_2023.06.1_modern_minimal.db')
cursor = conn.cursor()

# Create an index on the file_name column
cursor.execute("CREATE INDEX IF NOT EXISTS idx_file_name ON FILE (file_name)")

# Read file names from system-utils.txt
with open('system-utils.txt', 'r') as file:
    file_names = [line.rstrip('\n') for line in file]

total_files = len(file_names)
print(f"Total files to process: {total_files}")

# Create the query with batched file names
query = f"SELECT file_name, GROUP_CONCAT(sha1) as hashes FROM FILE WHERE file_name IN ({','.join('?' for _ in file_names)}) GROUP BY file_name"

# Execute the query with batched file names
cursor.execute(query, file_names)

# Process results incrementally
results_list = []
processed_files = 0

for row in cursor:
    result_dict = {
        'file_name': row[0],
        'hashes': row[1]
    }
    results_list.append(result_dict)

    processed_files += 1
    print(f"Processed files: {processed_files}/{total_files}")

# Convert the list of dictionaries to JSON
output_json = json.dumps(results_list)

# Write the JSON to a file
with open('output.json', 'w') as output_file:
    output_file.write(output_json)

# Close the database connection
cursor.close()
conn.close()

print("Query execution completed. Results written to output.json.")
