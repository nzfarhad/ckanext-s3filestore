'''
This script copies all resource files from a local FileStore directory
to a remote S3 bucket.

**It will not work for group images**

It requires SQLalchemy and Boto.

Please update the configuration details, all keys are mandatory except
AWS_STORAGE_PATH.

'''

import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import boto3
from botocore.exceptions import ClientError

import configparser
import ckan.plugins.toolkit as toolkit

# Configuration

CONFIG_FILE = '/etc/ckan/default/production.ini'

config = configparser.ConfigParser(strict=False)
config.read(CONFIG_FILE)
main_config = config['app:main']

BASE_PATH = main_config.get('ckan.storage_path', '/var/lib/ckan/default/resources')
SQLALCHEMY_URL = main_config.get('sqlalchemy.url', 'postgresql://user:pass@localhost/db')
if main_config.get('ckanext.s3filestore.aws_use_ami_role', False):
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None
else:
    AWS_ACCESS_KEY_ID = main_config.get('ckanext.s3filestore.aws_access_key_id', 'AKIxxxxxx')
    AWS_SECRET_ACCESS_KEY = main_config.get('ckanext.s3filestore.aws_secret_access_key', '+NGxxxxxx')
AWS_BUCKET_NAME = main_config.get('ckanext.s3filestore.aws_bucket_name', 'my-bucket')
AWS_STORAGE_PATH = ''
AWS_S3_ACL = main_config.get('ckanext.s3filestore.acl', 'public-read')
AWS_REGION_NAME = main_config.get('ckanext.s3filestore.aws_region_name', 'us-east-1')
AWS_CHECK_ACL = toolkit.asbool(main_config.get('ckanext.s3filestore.check_access_control_list', True))

resource_ids_and_paths = {}

for root, dirs, files in os.walk(BASE_PATH):
    if files:
        resource_id = root.split('/')[-2] + root.split('/')[-1] + files[0]
        resource_ids_and_paths[resource_id] = os.path.join(root, files[0])

print('Found {0} resource files in the file system'.format(
    len(resource_ids_and_paths.keys())))

engine = create_engine(SQLALCHEMY_URL)
connection = engine.connect()

resource_ids_and_names = {}

try:
    for resource_id, file_path in resource_ids_and_paths.iteritems():
        resource = connection.execute(text('''
            SELECT id, url, url_type
            FROM resource
            WHERE id = :id
        '''), id=resource_id)
        if resource.rowcount:
            _id, url, _type = resource.first()
            if _type == 'upload' and url:
                file_name = url.split('/')[-1] if '/' in url else url
                resource_ids_and_names[_id] = file_name.lower()
finally:
    connection.close()
    engine.dispose()

print('{0} resources matched on the database'.format(
    len(resource_ids_and_names.keys())))

def check_acl_support(s3_connection, bucket_name):
    if not AWS_CHECK_ACL:
        print("ACL support check disabled by configuration - defaulting to bucket owner enforced mode")
        return False
        
    try:
        s3_connection.get_bucket_acl(Bucket=bucket_name)
        print("S3 bucket supports ACLs")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'AccessControlListNotSupported':
            print("Warning: Bucket has Object Ownership set to 'Bucket owner enforced' - ACLs disabled")
            return False
        raise e

def main():
    s3_connection = boto3.client('s3',
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                              region_name=AWS_REGION_NAME)
                              
    supports_acl = check_acl_support(s3_connection, AWS_BUCKET_NAME)
    
    for resource_id in resource_ids_and_paths:
        key = resource_ids_and_paths[resource_id]
        print('Uploading {0} to S3...'.format(key))
        try:
            kwargs = {
                'Bucket': AWS_BUCKET_NAME,
                'Key': key,
                'Body': open(resource_ids_and_paths[resource_id], 'rb')  # Use binary mode for safety
            }
            if supports_acl:
                kwargs['ACL'] = AWS_S3_ACL
            s3_connection.put_object(**kwargs)
            print('Finished uploading {0} to S3'.format(key))
        except Exception as e:
            print('Error uploading {0} to S3: {1}'.format(key, e))
            continue

if __name__ == "__main__":
    main()
