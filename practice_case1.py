from google.cloud import storage
import urllib.request

# Setting credentials using the downloaded JSON file
client = storage.Client.from_service_account_json(json_credentials_path='data-fellowship7-83ba187356c9.json')

# Define Variable Name
bucket_name = 'practice-case1-mhmdzaky'
destination_name = 'logo_telu.jpg'
file = urllib.request.urlopen('https://upload.wikimedia.org/wikipedia/commons/0/03/Logo_Telkom_University_potrait.png')

try:
    # Creating bucket object
    bucket = client.get_bucket(bucket_name)

    # Name of the object to be stored in the bucket

    blob = bucket.blob(destination_name)

    # Upload file from url
    blob.upload_from_string(file.read(), content_type='image/jpg')
    
    print('File Uploaded!')
except:
    print('Upload Failed')
