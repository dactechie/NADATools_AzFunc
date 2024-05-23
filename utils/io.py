import csv
from io import StringIO
from pathlib import Path

# import csv

# def write_csv(location:Path, filename:str, data):
#   full = location.joinpath(filename)
#   with open(full, 'w', newline='') as file:
#       writer = csv.writer(file)      
#       # Write each row of data to the CSV file
#       writer.writerows(data)

def write_csv(location: Path, filename: str, data):
    full = location.joinpath(filename)
    with open(full, 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerows([row] for row in data)

def write_stream_to_csv(location: Path, filename: str, csv_stream_data):
     # Create a StringIO object from the CSV data
    csv_string = StringIO(csv_stream_data)


    # Create a CSV reader object
    csv_reader = csv.reader(csv_string)

    # Process the CSV data
    output_data = []
    for row in csv_reader:
        # Perform operations on each row of the CSV data
        # For example, concatenate the values in each row
        output_row = ','.join(row)
        output_data.append(output_row)
    # output_filename = 'processed_' + filename        
    write_csv(location, filename, output_data)
    
    
   

    # full = location.joinpath(filename)
    # with open(full, 'w', encoding='utf-8') as file:
    #     # Write each character on a separate line
    #     for char in csv_stream_data:
    #         file.write(char + '\n')

# def write_csv(location: Path, filename: str, data):
#     full = location.joinpath(filename)
#     with open(full, 'w', newline='', encoding='utf-8') as file:
#         writer = csv.writer(file, lineterminator='\n')
#         # Write each character as a separate row in the CSV file
#         for char in data:
#             writer.writerow([char])