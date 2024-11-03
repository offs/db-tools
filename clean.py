import csv
import json
import sqlite3
import os

def get_delimiter(file_path):
    with open(file_path, 'r') as file:
        dialect = csv.Sniffer().sniff(file.read(1024))
        delimiter = dialect.delimiter
        if delimiter is None:
            print(f"Could not detect delimiter, defaulting to comma (,)")
            delimiter = ','
        return delimiter

def get_key(row, name_col):
    key = row[name_col]
    if '@' in key:
        return key  # email
    elif ' ' in key:
        return key  # full name
    else:
        return key  # user or anything else that can be used as a key
    
def get_name_column(headers):
    name_keywords = ['name', 'username', 'email']
    for i, header in enumerate(headers):
        if any(keyword in header.lower() for keyword in name_keywords):
            return i
    return 0  # Default to first column if no match found


def read_csv(file_path):
    data = {}
    print(f"Opening file: {file_path}")
    
    delimiter = get_delimiter(file_path)
    
    with open(file_path, 'r') as file:
        for row_num, line in enumerate(file, 1):
            parts = line.strip().split(delimiter)
            if len(parts) >= 3:
                url, identifier, password = [part.strip() for part in parts[:3]]
                key = get_key({'identifier': identifier}, 'identifier')
                if key:
                    if key not in data:
                        data[key] = []
                    data[key].append(password)
                else:
                    print(f"Skipped: Invalid identifier (Row {row_num})")
            else:
                print(f"Skipped: Row has less than 3 parts (Row {row_num})")
    
    print(f"Finished processing. Total entries: {len(data)}")
    return data

def read_sql(file_path):
    data = {}
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_name = cursor.fetchone()[0]
    cursor.execute(f"SELECT * FROM {table_name}")
    headers = [description[0] for description in cursor.description]
    name_col = get_name_column(headers)
    for row in cursor.fetchall():
        if any(row):
            key = get_key(row, name_col)
            values = [str(value).strip() for i, value in enumerate(row) if i != name_col and str(value).strip()]
            if values:  # Only add if there are non-empty values
                data[key] = values
    conn.close()
    print (f"Finished processing. Total entries: {len(data)}")
    return data

def read_txt(file_path):
    data = {}
    delimiter = get_delimiter(file_path)  # Detect delimiter for txt file
    with open(file_path, 'r', encoding='utf-8') as txtfile:
        for line in txtfile:
            parts = line.strip().split(delimiter)  # Split line using detected delimiter
            key = get_key(parts, 0)
            values = [value.strip() for value in parts[1:] if value.strip()]
            if values:  # Only add if there are non-empty values
                data[key] = values
    print (f"Finished processing. Total entries: {len(data)}")
    return data

def write_json(data, output_file):
    with open(output_file, 'w') as jsonfile:
        json.dump(data, jsonfile, indent=2)

def main():
    input_file = input("Enter the input file path: ")
    output_file = os.path.splitext(input_file)[0] + 'out.json'
    
    file_type = os.path.splitext(input_file)[1].lower()
    
    if file_type == '.csv':
        data = read_csv(input_file)
    elif file_type == '.db' or file_type == '.sqlite':
        data = read_sql(input_file)
    elif file_type == '.txt':
        data = read_txt(input_file)
    else:
        print("Unsupported file type")
        return
    
    write_json(data, output_file)
    print(f"Data has been written to {output_file}")

if __name__ == "__main__":
    main()