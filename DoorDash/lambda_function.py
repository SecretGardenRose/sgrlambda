import boto3

from email import policy
from email.parser import BytesParser

s3 = boto3.client('s3')
textract = boto3.client('textract')


def lambda_handler(event, context):

    print ("entered function 1")
    # Step 1: Extract the message ID from SES event
    ses_record = event['Records'][0]['ses']
    message_id = ses_record['mail']['messageId']
    
    #message_id = 'qtg8divbtknfuu2dtrvv6g8fukabvps2v4qqf681'
    #message_id = 'eblinuob0u6iqojngvbvf1lfcm22utbtq1ifdi01' #Amanda O

    # Step 2: Build the S3 object path
    bucket_name = 'ses-sgrcredit'
    object_key = f'{message_id}'

    print (object_key)
    # Step 3: Read raw email content from S3
    s3_object = s3.get_object(Bucket=bucket_name, Key=object_key)
    raw_email = s3_object['Body'].read()

    print ("hello")
    # Step 4: Parse raw email using the built-in email parser
    msg = BytesParser(policy=policy.default).parsebytes(raw_email)

    # Extract metadata
    subject = msg['subject']
    sender = msg['from']
    recipient = msg['to']

    # Extract email body (plain text and HTML if present)
    plain_body = None
    html_body = None

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()

            if content_type == 'text/plain':
                plain_body = part.get_content()
                
            elif content_type == 'text/html':
                html_body = part.get_content()
                
            elif content_type == 'application/pdf':
                print (part.get_content_disposition())
                if part.get_content_disposition() == 'attachment' and part.get_filename().endswith('.pdf'):

                    print (part.get_filename())
                    print (part.get_content_type())
                    print (part.get_content_disposition())
                    bytes_data = part.get_payload(decode=True)

                    response = textract.analyze_document(
                        Document={'Bytes': bytes_data},
                        FeatureTypes=["TABLES"]
                    )

                    print_tables(response)





    else:
        content_type = msg.get_content_type()
        if content_type == 'text/plain':
            plain_body = msg.get_content()
        elif content_type == 'text/html':
            html_body = msg.get_content()

    # Debug output (only print first 200 characters for safety)
    print(f"From: {sender}")
    print(f"To: {recipient}")
    print(f"Subject: {subject}")
    print(f"Plain Body: {(plain_body or '')[:200]}")
    print(f"HTML Body: {(html_body or '')[:200]}")

    return {
        'statusCode': 200,
        'body': 'Email successfully parsed.'
    }


def get_text_from_relationships(block_map, relationships):
    text = ''
    if not relationships:
        return text

    for rel in relationships:
        if rel['Type'] == 'CHILD':
            for child_id in rel['Ids']:
                word = block_map.get(child_id)
                if word['BlockType'] == 'WORD':
                    text += word['Text'] + ' '
                elif word['BlockType'] == 'SELECTION_ELEMENT':
                    if word['SelectionStatus'] == 'SELECTED':
                        text += 'X '
    return text.strip()

def print_tables(response):
    blocks = response['Blocks']
    block_map = {block['Id']: block for block in blocks}

    for block in blocks:
        if block['BlockType'] == 'TABLE':
            print("\n=== Table ===")
            # Map cells by row and column
            table = {}
            for relationship in block.get('Relationships', []):
                if relationship['Type'] == 'CHILD':
                    for cell_id in relationship['Ids']:
                        cell = block_map[cell_id]
                        if cell['BlockType'] == 'CELL':
                            row = cell['RowIndex']
                            col = cell['ColumnIndex']
                            content = get_text_from_relationships(block_map, cell.get('Relationships', []))
                            table[(row, col)] = content

            # Get max row/column size
            max_row = max(k[0] for k in table.keys())
            max_col = max(k[1] for k in table.keys())

            for r in range(1, max_row + 1):
                row_text = []
                for c in range(1, max_col + 1):
                    row_text.append(table.get((r, c), ''))
                print('\t'.join(row_text))