import csv

def parse_csv_to_name_category_dict(filename):
    result = {}
    with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            #print (row)
            name = row.get("Name")
            category = row.get("Category")
            if name is not None and category is not None:
                result[name] = category
    return result


def add_category_column(input_file, output_file, name_to_category):
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_file, mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['category']  # Add new column

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            name = row.get('Name')
            row['category'] = name_to_category.get(name, '')  # Default to empty string
            writer.writerow(row)


# Example usage
if __name__ == "__main__":
    filename = '/Users/peterlu/Downloads/202506-Transactions.csv'  # Replace with your actual file path
    name_category_dict = parse_csv_to_name_category_dict(filename)

    inputFilename = '/Users/peterlu/Downloads/202505-Transactions-input.csv'
    outputFilename = '/Users/peterlu/Downloads/202505-Transactions.csv'

    add_category_column(inputFilename, outputFilename, name_category_dict)

