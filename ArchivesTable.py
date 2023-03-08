import json
import os
from os.path import exists

import boto3
from PyQt6 import QtWidgets, uic
from PyQt6.QtCore import QSize
from PyQt6.QtGui import QStandardItem, QStandardItemModel
from PyQt6.QtWidgets import QMessageBox

CREDENTIALS = {
    'access_key_id': 'access-key-id',
    'secret_access_key': 'secret-access-key',
    'vault_name': 'name',
    'account_id': 'id',
    'bucket': 'bucket-name'
}

basedir = os.path.dirname(__file__)


class ArchivesTable(QtWidgets.QMainWindow):

    def __init__(self, glacier_resource, vault, main_window):
        super(ArchivesTable, self).__init__(parent=main_window)
        self.main_window = main_window
        self.loadUi()
        self.glacier_resource = glacier_resource
        self.s3 = boto3.resource('s3',
                                 aws_access_key_id=CREDENTIALS['access_key_id'],
                                 aws_secret_access_key=CREDENTIALS['secret_access_key'])
        self.vault = vault
        self.jobs = self.vault.jobs.all()
        self.setWindowTitle('Archives Table')

        path_to_archives_json = os.path.join(basedir, 'storage/archives.json')
        if not exists(path_to_archives_json):
            bucket = self.s3.Bucket(CREDENTIALS['bucket'])
            bucket.download_file('archives.json', path_to_archives_json)

        if os.stat(path_to_archives_json).st_size != 0:
            with open(path_to_archives_json, 'r') as archives:
                try:
                    archives_list = json.load(archives)
                except:
                    print('invalid json format')
                    archives_list = []
        else:
            archives_list = []

        self.item_model = QStandardItemModel(self)
        self.item_model.setHorizontalHeaderLabels(['ID', 'Description'])
        for archive in archives_list:
            if archive:
                self.item_model.appendRow([QStandardItem(archive['id']), QStandardItem(archive['description'])])
        self.archives_table.setModel(self.item_model)
        self.archives_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.MultiSelection)
        self.archives_table.verticalHeader().hide()
        hh = self.archives_table.horizontalHeader()
        vh = self.archives_table.verticalHeader()
        size = QSize(hh.length() + vh.sizeHint().width(), vh.length() + hh.sizeHint().height() + 25)
        self.setMinimumSize(size)
        self.archives_delete_btn.clicked.connect(self.on_button_pressed)
        self.archives_download_btn.clicked.connect(self.on_button_pressed)

    def loadUi(self):
        uic.loadUi(os.path.join(basedir, 'storage/ui/archives_table.ui'), self)

    def update_table(self):
        if self.item_model.rowCount():
            self.item_model.setRowCount(0)
            self.item_model.setColumnCount(0)
        else:
            self.item_model.removeRow(0)
            self.item_model.removeColumn(0)
        with open(os.path.join(basedir, 'storage/archives.json'), 'r') as archives:
            archives_list = json.load(archives)
        self.item_model.setHorizontalHeaderLabels(['ID', 'Description'])
        for archive in archives_list:
            if archive:
                self.item_model.appendRow([QStandardItem(archive['id']), QStandardItem(archive['description'])])

    def start_archive_retrieval_job(self, archive_id):
        self.glacier_resource.Archive(CREDENTIALS['account_id'], CREDENTIALS['vault_name'],
                                      archive_id).initiate_archive_retrieval()

    def get_archive_extension_and_description(self, archive_id):
        archives_json = open(os.path.join(basedir, 'storage/archives.json'), 'r')
        archive_list = json.load(archives_json)
        for archive in archive_list:
            if archive['id'] == archive_id:
                archive_file_extension = archive['extension']
                archive_description = archive['description']
                return archive_file_extension, archive_description
        return None, ''

    def remove_archive(self, archive_id):
        account_id = CREDENTIALS['account_id']
        vault_name = CREDENTIALS['vault_name']
        archive = self.glacier_resource.Archive(account_id, vault_name, archive_id)
        response = archive.delete()
        with open(os.path.join(basedir, 'storage/logs/deleted_archive_response_log.txt'), 'a') as delete_log:
            json_formatted_output = json.dumps(response, indent=4, sort_keys=True)
            delete_log.write(json_formatted_output)
        with open(os.path.join(basedir, 'storage/archives.json'), 'r') as archives_json:
            archive_list = json.load(archives_json)
            new_archive_list = []
            for archive in archive_list:
                if archive['id'] == archive_id:
                    del archive
                else:
                    new_archive_list.append({'id': archive['id'], 'description': archive['description'], "extension": archive['extension'], "size": archive['size']})
        with open(os.path.join(basedir, 'storage/archives.json'), 'w') as archives:
            json.dump(new_archive_list, archives)
        response = self.s3.Bucket(CREDENTIALS['bucket']).upload_file(os.path.join(basedir, 'storage/archives.json'), 'archives.json')
        with open(os.path.join(basedir, f'storage/logs/replaced_file_of_bucket_{CREDENTIALS["bucket"]}_response_log.txt'), 'a') as delete_log:
            json_formatted_output = json.dumps(response, indent=4, sort_keys=True)
            delete_log.write(json_formatted_output)
        self.update_table()

    def download_archive_retrieval_output(self, archive_id, path=None):
        retrieval_job = None
        archive_file_extension, archive_description = self.get_archive_extension_and_description(archive_id)
        if not path:
            if archive_description == '':
                path = os.path.join(basedir, 'storage/output/') + archive_id
            else:
                path = os.path.join(basedir, 'storage/output/') + archive_description
        if os.path.splitext(path)[1] == '':
            filename = path + archive_file_extension
        else:
            filename = path
        for job in self.jobs:
            if job.archive_id == archive_id:
                retrieval_job = job
                break
        archive_output = retrieval_job.get_output()
        with open(filename, 'wb') as archive_output_file_raw:
            output_raw = archive_output['body'].read()
            archive_output_file_raw.write(output_raw)

    def archive_retrieval_status(self, archive_id):
        for job in self.jobs:
            if job.archive_id == archive_id:
                return job.completed, True
        return False, False

    def on_button_pressed(self):
        button_id = self.sender().objectName()

        if button_id == self.archives_download_btn.objectName():

            if len(self.archives_table.selectedIndexes()) == 1:
                row = [self.archives_table.selectedIndexes()[0].row()]
                archive_id = self.item_model.index(row[0], 0).data()
                self.handle_single_archive_download_request(archive_id)

            elif len(self.archives_table.selectedIndexes()) > 1:
                rows = [x.row() for x in self.archives_table.selectedIndexes()]
                archive_ids = []
                for row in rows:
                    archive_id = self.item_model.index(row, 0).data()
                    archive_ids.append(archive_id)
                self.handle_multi_archive_download_request(archive_ids)

        elif button_id == self.archives_delete_btn.objectName():

            if len(self.archives_table.selectedIndexes()) == 1:
                row = [self.archives_table.selectedIndexes()[0].row()]
                archive_id = self.item_model.index(row[0], 0).data()
                confirmation = QMessageBox.question(self,
                                                    'Delete Archive',
                                                    'You are about to delete an archive. Are you sure you want to continue',
                                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                                                    QMessageBox.StandardButton.Cancel)
                if confirmation == QMessageBox.StandardButton.Yes:
                    self.remove_archive(archive_id)
            elif len(self.archives_table.selectedIndexes()) > 1:
                rows = [x.row() for x in self.archives_table.selectedIndexes()]
                confirmation = QMessageBox.question(self,
                                                    'Delete Archive',
                                                    'You are about to delete multiple archives. Are you sure you want to continue',
                                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                                                    QMessageBox.StandardButton.Cancel)
                if confirmation == QMessageBox.StandardButton.Yes:
                    for row in rows:
                        archive_id = self.item_model.index(row, 0).data()
                        self.remove_archive(archive_id)

    def handle_single_archive_download_request(self, archive_id):
        retrieval_completed, retrieval_started = self.archive_retrieval_status(archive_id)
        if not retrieval_started:
            confirmation = QMessageBox.question(self,
                                                'Download Archive',
                                                'You are about to start an archive retrieval for one archive. Note that this might take a while (3.5 - 4 hours). After starting the retrieval, wait and then try to download the archive again.',
                                                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                                                QMessageBox.StandardButton.Cancel)
            if confirmation == QMessageBox.StandardButton.Yes:
                self.start_archive_retrieval_job(archive_id)
        elif retrieval_completed:
            confirmation = QMessageBox.question(self,
                                                'Download Archive',
                                                'Archive is ready for download. Would you like to download it?',
                                                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                                                QMessageBox.StandardButton.Cancel)
            if confirmation == QMessageBox.StandardButton.Yes:
                extension = self.get_archive_extension_and_description(archive_id)[0]
                file_name = QtWidgets.QFileDialog.getSaveFileName(self,
                                                                  'Select where the file will be saved',
                                                                  filter=f'({extension})',
                                                                  options=QtWidgets.QFileDialog.Option.DontUseNativeDialog)
                if file_name[0] != '':
                    self.download_archive_retrieval_output(archive_id, path=file_name[0])
        else:
            QMessageBox.question(self,
                                 'Archive is not yet ready',
                                 'Archive is NOT yet ready to be downloaded. Please Try again later',
                                 QMessageBox.StandardButton.Yes,
                                 QMessageBox.StandardButton.Yes)

    def handle_multi_archive_download_request(self, archive_id_array):
        archives_with_completed_jobs = []
        archives_with_in_progress_jobs = []
        archives_with_not_started_jobs = []

        for archive_id in archive_id_array:
            retrieval_completed, retrieval_started = self.archive_retrieval_status(archive_id)
            if retrieval_completed:
                archives_with_completed_jobs.append(archive_id)
            elif retrieval_started:
                archives_with_in_progress_jobs.append(archive_id)
            else:
                archives_with_not_started_jobs.append(archive_id)
        messagebox_options = {
            'title': 'Multi-Archive download started',
            'message': 'Your archives will be available shortly, in the storage directory',
            'buttons': QMessageBox.StandardButton.Ok,
            'default_button': QMessageBox.StandardButton.Ok
        }
        if len(archives_with_not_started_jobs) != 0:
            messagebox_options['title'] = 'Some archive retrievals have not started'
            messagebox_options['message'] = 'Some of the archives have not been retrieved. Would you like to start retrieving them? You will be notified by email when the retrievals are completed.\n(It takes 3.5 - 4 hours for the archives to be retrieved.\n!!Note that all the files will be saved in the storage directory!!)'
            messagebox_options['buttons'] = QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
            messagebox_options['default_button'] = QMessageBox.StandardButton.Yes
        elif len(archives_with_in_progress_jobs) != 0:
            if len(archives_with_completed_jobs) != 0:
                messagebox_options['title'] = 'Some archive retrievals have not finished yet'
                messagebox_options['message'] = 'Not all the archives are ready for downloading. You will be notified by email when the retrievals are completed. Would you like to start downloading the ready ones?\n(!!Note that all the files will be saved in the storage directory!!)'
                messagebox_options['buttons'] = QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
                messagebox_options['default_button'] = QMessageBox.StandardButton.Yes
            else:
                messagebox_options['title'] = 'Some archive retrievals have not finished yet'
                messagebox_options['message'] = 'No archives are ready for downloading. You will be notified by email when the retrievals are completed. You can expect your files around 3.5 - 4 hours after starting the retrieval job'
                messagebox_options['buttons'] = QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
                messagebox_options['default_button'] = QMessageBox.StandardButton.Yes
        elif len(archives_with_completed_jobs) == 0:
            messagebox_options['title'] = 'Archives are not ready'
            messagebox_options['message'] = 'There currently no archives ready to be downloaded. You will be notified by email when the retrievals are completed. You can expect your files around 3.5 - 4 hours after starting the retrieval job'
            messagebox_options['buttons'] = QMessageBox.StandardButton.Ok
            messagebox_options['default_button'] = QMessageBox.StandardButton.Ok
        confirmation = QMessageBox.question(self,
                                            messagebox_options['title'],
                                            messagebox_options['message'],
                                            messagebox_options['buttons'],
                                            messagebox_options['default_button'])
        if confirmation == QMessageBox.StandardButton.Yes or confirmation == QMessageBox.StandardButton.Ok:
            # file_name = QtWidgets.QFileDialog.getSaveFileName(self,
            #                                                   'Select where the files will be saved',
            #                                                   filter='All Files (*)',
            #                                                   options=QtWidgets.QFileDialog.Option.DontUseNativeDialog)
            # if file_name[0] != '':
            for archive in archives_with_completed_jobs:
                self.download_archive_retrieval_output(archive)
            for archive in archives_with_not_started_jobs:
                self.start_archive_retrieval_job(archive)
