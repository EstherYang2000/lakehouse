log_file_path = "./91baf1eb-65d7-422c-8366-679f7cea7271-0_20240717131718847.log.2_0-22-70"

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
