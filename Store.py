import os
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def store_to_drive():
    logging.info("Starting upload to Google Drive using service account")

    try:
        file_path = "/home/dcontreras/Workshop002/temp/merged_dataset.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError("Merged dataset not found.")

        credentials_path = "/home/dcontreras/Workshop002/credentials/service_account.json"
        folder_id = "1aP8Fv9kQzl_CnP9us6S33Dde1fsyyCCu"

        scopes = ['https://www.googleapis.com/auth/drive.file']
        creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=scopes)

        service = build('drive', 'v3', credentials=creds)

        # Nombre del archivo a subir
        file_name = 'merged_dataset.csv'

        # Verificar si el archivo ya existe en la carpeta de Google Drive
        existing_files = service.files().list(
            q=f"name='{file_name}' and '{folder_id}' in parents",
            spaces='drive',
            fields="files(id, name)"
        ).execute()

        # Si el archivo existe, eliminarlo
        if existing_files.get('files', []):
            file_id = existing_files['files'][0]['id']
            logging.info(f"File found in Drive. Deleting file with ID: {file_id}")
            service.files().delete(fileId=file_id).execute()
            logging.info("Old file deleted successfully.")

        # Subir el nuevo archivo
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }

        media = MediaFileUpload(file_path, mimetype='text/csv')

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()

        logging.info(f"File uploaded successfully. File ID: {file.get('id')}")
        return True

    except Exception as e:
        logging.error(f"Error uploading file to Google Drive: {e}")
        return False
