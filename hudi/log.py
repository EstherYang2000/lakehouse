log_file_path = "./0dd14b4c-5d85-4b28-ac59-e07fe1bf281d-0_20240719111906465.log.1_0-14-67"

try:
    with open(log_file_path, "r", encoding="utf-8", errors="ignore") as log_file:
        # Read all lines into a list
        log_lines = log_file.readlines()
        
        # Process each line or print them
        for line in log_lines:
            print(line.strip())  # Strip newline characters for clean output

except FileNotFoundError:
    print(f"Error: Log file '{log_file_path}' not found.")
except IOError as e:
    print(f"Error reading log file: {e}")
