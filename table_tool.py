#!/usr/bin/env python
import boto
import time
from optparse import OptionParser

conn = boto.connect_dynamodb()

tables = [
    {
        'name': 'nicovideo_videos',
        'schema': conn.create_schema(hash_key_name='video_id', hash_key_proto_value='a')
    },
    {
        'name': 'nicovideo_tags',
        'schema': conn.create_schema(hash_key_name='tag_name', hash_key_proto_value='a')
    },
]

def parseopt():
    p = OptionParser()
    p.add_option("-c", "--create-tables", action="store_true", dest="create", default=False, help="create tables.")
    p.add_option("-d", "--delete-tables", action="store_true", dest="delete", default=False, help="delete tables.")
    return p.parse_args()

def create_tables(read_units=3, write_units=5):
    for t in tables:
        conn.create_table(name=t['name'], schema=t['schema'], read_units=3, write_units=5)
        while True:
            table_info = conn.describe_table(t['name'])
            if table_info['Table']['TableStatus'] == 'ACTIVE':
                break
            time.sleep(1)

def delete_tables():
    for t in tables:
        conn.delete_table(conn.get_table(t['name']))
        while True:
            try:
                table_info = conn.describe_table(t['name'])
            except boto.exception.DynamoDBResponseError:
                break
            else:
                time.sleep(1)

def describe_tables():
    for t in tables:
        table_info = conn.describe_table(t['name'])
        print table_info

def main():
    opts, args = parseopt()    

    read_units = 3
    write_units = 5
    if args and len(args) == 2:
        read_units = args[0]
        write_units = args[1]

    if opts.create:
        create_tables(read_units, write_units)
    elif opts.delete:
        delete_tables()
    else:
        describe_tables()

if __name__ == "__main__":
    main()
