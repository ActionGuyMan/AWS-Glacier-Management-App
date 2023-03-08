import json
import logging
import os
import sys

import boto3
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox
from botocore.exceptions import ClientError

from ArchivesTable import ArchivesTable

logger = logging.getLogger(__name__)

CREDENTIALS = {
    'access_key_id': 'access-key-id',
    'secret_access_key': 'secret-access-key',
    'vault_name': 'name',
    'account_id': 'id',
    'region': 'region',
    'bucket': 'bucket-name'
}

basedir = os.path.dirname(__file__)


class MainWindow(QtWidgets.QMainWindow):

    def __init__(self):
        super(MainWindow, self).__init__()
        self.loadUi()
        self.setWindowTitle('AWS Glacier Archive app')

        self.glacier = boto3.resource('glacier',
                                      region_name=CREDENTIALS['region'],
                                      aws_access_key_id=CREDENTIALS['access_key_id'],
                                      aws_secret_access_key=CREDENTIALS['secret_access_key'])
        self.vault = self.glacier.Vault(CREDENTIALS['account_id'], CREDENTIALS['vault_name'])
        self.archives_table_window = ArchivesTable(self.glacier, self.vault, self)
        self.archive_files_btn.clicked.connect(self.select_files_to_upload)
        self.retrieve_file_btn.clicked.connect(self.show_list_of_archives)
        self.retrieve_files_btn.clicked.connect(self.start_retrieve_job)
        self.download_recent_job_output_btn.clicked.connect(self.download_most_recent_job_output)

    def loadUi(self):
        uic.loadUi(os.path.join(basedir, 'storage/ui/aws_archive.ui'), self)

    def select_files_to_upload(self):
        file_name = QtWidgets.QFileDialog.getOpenFileName(self,
                                                          'Select File(s) to upload',
                                                          options=QtWidgets.QFileDialog.Option.DontUseNativeDialog)
        if file_name[0] != '':
            self.upload_archive(file_name[0])

    def upload_archive(self, file_to_archive):
        print(file_to_archive)
        file_size = os.stat(file_to_archive).st_size
        description, ok = QtWidgets.QInputDialog.getText(self, 'Archive Description',
                                                         'Enter a description (recommended):')

        with open(file_to_archive, 'rb') as upload_file:
            if file_size/(1024 * 1024) < 100:
                archive = self.vault.upload_archive(body=upload_file)
            else:
                archive = self.big_archive_upload(file_to_archive)
        file_extension = os.path.splitext(file_to_archive)[1]
        new_archive_entry = [{"id": archive.id, "description": description, "extension": file_extension, "size": f"{file_size/(1024 * 1024)} MB"}]
        self.add_to_archives_json(new_archive_entry)

    def start_retrieve_job(self):
        confirmation = QMessageBox.question(self,
                                            'Retrieve all archives',
                                            'You are about to initiate a retrieval of all available archives. ' + \
                                            'Note that, this operation will take a while to complete.',
                                            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                                            QMessageBox.StandardButton.Cancel)
        if confirmation == QMessageBox.StandardButton.Yes:
            self.vault.initiate_inventory_retrieval()

    def download_most_recent_job_output(self):
        most_recent_inventory_retrieval_job = None
        for job in self.vault.succeeded_jobs.all():
            if job.action == 'InventoryRetrieval':
                most_recent_inventory_retrieval_job = job
        if most_recent_inventory_retrieval_job is None:
            QMessageBox.question(self,
                                 'Something went wrong',
                                 'There are no completed jobs to get output from. Try again later.',
                                 QMessageBox.StandardButton.Ok,
                                 QMessageBox.StandardButton.Ok)
        else:
            QMessageBox.question(self,
                                 'Download will now start',
                                 'The most recent inventory will now begin downloading.',
                                 QMessageBox.StandardButton.Ok,
                                 QMessageBox.StandardButton.Ok)
            output = self.get_job_output(most_recent_inventory_retrieval_job)
            with open(os.path.join(basedir, 'storage/inventory.json'), 'w') as json_output_file:
                json_output = json.loads(output)
                json_formatted_output = json.dumps(json_output, indent=4, sort_keys=True)
                json_output_file.write(json_formatted_output)
                new_archive_entries = []
                for archive in json_output['ArchiveList']:
                    new_archive_entries.append(
                        {'id': archive['ArchiveId'], 'description': archive['ArchiveDescription'], "extension": ""})
                self.add_to_archives_json(new_archive_entries)
            QMessageBox.question(self,
                                 'Download has finished',
                                 'Inventory has been downloaded. Check the Archives Table to manage your archives.',
                                 QMessageBox.StandardButton.Ok,
                                 QMessageBox.StandardButton.Ok)

    def big_archive_upload(self, file_to_archive):
        pass

    def get_job_output(self, job):
        try:
            response = job.get_output()
            out_bytes = response['body'].read()
            logger.info("Read %s bytes from job %s.", len(out_bytes), job.id)
            if 'archiveDescription' in response:
                logger.info(
                    "These bytes are described as '%s'", response['archiveDescription'])
        except ClientError:
            logger.exception("Couldn't get output for job %s.", job.id)
            raise
        else:
            return out_bytes

    def show_list_of_archives(self):
        self.archives_table_window.show()

    def retrieve_single_archive(self, archive_id):
        archive = self.glacier.Archive(CREDENTIALS['account_id'], CREDENTIALS['vault_name'], archive_id)
        archive.initiate_archive_retrieval()

    def add_to_archives_json(self, archive_array):
        with open(os.path.join(basedir, 'storage/archives.json'), 'r') as archives:
            past_archives = json.load(archives)
            for archive in archive_array:
                past_archives.append(archive)
        with open(os.path.join(basedir, 'storage/archives.json'), 'w') as archives:
            json.dump(past_archives, archives)
        response = self.archives_table_window.s3.Bucket(CREDENTIALS['bucket']).upload_file(os.path.join(basedir, 'storage/archives.json'),
                                                                                           'archives.json')
        with open(os.path.join(basedir, f'storage/logs/replaced_file_of_bucket_{CREDENTIALS["bucket"]}_response_log.txt'), 'a') as delete_log:
            json_formatted_output = json.dumps(response, indent=4, sort_keys=True)
            delete_log.write(json_formatted_output)
        self.archives_table_window.update_table()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    app.exec()
