import logging
import os
from io import BytesIO
from dotenv import find_dotenv, load_dotenv
from azure.storage.blob import BlobServiceClient
# import mylogging

# logging = mylogging.get('azure.storage')



class AzureBlobQuery(object):
  def __init__(self):
      load_dotenv(find_dotenv())
      # self.access_key = os.getenv("TABLES_PRIMARY_STORAGE_ACCOUNT_KEY")
      # self.endpoint_suffix = os.getenv("TABLES_STORAGE_ENDPOINT_SUFFIX")
      # self.account_name = os.getenv("TABLES_STORAGE_ACCOUNT_NAME")
      # self.endpoint = "{}.table.{}".format(self.account_name, self.endpoint_suffix)
      self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "Help") # AZURE_BLOBSTORAGE_CONNECTION_STRING
      if self.connection_string == 'Help':
        logging.error("Blob Connection string not found.")
        self.connection_string = ""
        # st.warning("An error occurred while loading the data. Please try again later.")
        return None

  # @st.cache
  def load_data(self, blob_url):
      
      try:
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        blob_client = blob_service_client.get_blob_client(container='reporting', blob=blob_url)
        blob_data = blob_client.download_blob().readall()

        logging.debug(f"Loaded blob bytes of length {len(blob_data)}.")
        # Assuming the blob_data is a parquet file
        # data = pd.read_parquet(BytesIO(blob_data))
        return BytesIO(blob_data)
      
      except Exception as e:
        # Log the exception
        logging.error(f"An error occurred while loading data from Blob Storage: {str(e)}")
        # You may want to display a user-friendly message in the Streamlit app
        # st.warning("An error occurred while loading the data. Please try again later.")
        return None       
       
# data = load_data('path/to/yourfile.parquet')
