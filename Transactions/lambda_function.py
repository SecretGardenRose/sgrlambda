import boto3


import csv
from datetime import datetime

# === Configuration ===
# file1 = 'file1.csv'
# file2 = 'file2.csv'
# output_file = 'combined_sorted.csv'

# Mapping from file2's headers to file1's headers
# Format: {file1_header: file2_header}
column_mapping = {
    'Card': 'Details',
    'Post Date': "Posting Date",
    'Description': 'Description',
    'Amount': 'Amount',
    'Type': 'Type'
}

def read_and_map(file_path, header_map, final_headers):
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        mapped_rows = []

        for row in reader:
            new_row = {}
            for header in final_headers:
                if header in header_map:
                    old_key = header_map[header]
                    new_row[header] = row.get(old_key, '')
                else:
                    new_row[header] = ''  # Fill with empty string if not mapped
            mapped_rows.append(new_row)
        return mapped_rows


# Sort by Post Date
def parse_date(row):

    try:
        d = datetime.strptime(row['Post Date'], '%m/%d/%y')  # Adjust format if needed
        print(d)
        return d
    except Exception:
        return datetime.min  # Fallback for invalid or missing dates


def parse_csv_to_name_category_dict(filename, result):

    with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            name = row.get("Name")
            category = row.get("category")
            if name is not None and category is not None and name not in result:
                result[name] = category
    
def add_category_column(input_file, name_to_category):

    with open(afile, mode='r', newline='', encoding='utf-8') as infile, \
        open(afile+".out.csv", mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['category']  # Add new column

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            name = row.get('Description')
            row['category'] = name_to_category.get(name, '')  # Default to empty string
            writer.writerow(row)


# Example usage
if __name__ == "__main__":
    name_category_dict = {}

    s3 = boto3.client('s3')
    bucket_name = 'monthly-transactions'

    # Use paginator to handle large lists of objects
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)

    for page in pages:
        for obj in page.get('Contents', []):
            print(obj['Key'])  # This is the filename (key)

    sys.exit(0)

    filename = '/Users/peterlu/Downloads/202506-Transactions.csv'  # Replace with your actual file path
    parse_csv_to_name_category_dict(filename, name_category_dict)

    filename = '/Users/peterlu/Downloads/202505-Transactions-1.csv'
    parse_csv_to_name_category_dict(filename, name_category_dict)

    cc = '/Users/peterlu/Downloads/Chase8507_Activity20250501_20250531_20250712.CSV'
    checking = '/Users/peterlu/Downloads/Chase3738_Activity_20250712.CSV'
    
    # Read first file directly
    with open(cc, newline='', encoding='utf-8') as f1:
        reader = csv.DictReader(f1)
        file1_headers = reader.fieldnames
        
        data1 = list(reader)

        # Process file2
        data2 = read_and_map(checking, column_mapping, file1_headers)
        
        combined = data1 + data2

        combined_sorted = sorted(combined, key=parse_date)

        for row in combined_sorted:
            name = row.get('Description')
            row['Memo'] = name_category_dict.get(name, '')  # Default to empty string

        with open('out1234.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=file1_headers)
            writer.writeheader()
            writer.writerows(combined_sorted)


    #add_category_column(inputfiles, name_category_dict)

