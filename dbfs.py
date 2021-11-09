from base64 import b64encode
import requests

class DbfsApi:
    def __init__(self, provCtx):
        self.provCtx = provCtx

    def create(self, path, overwrite=True):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/dbfs/create'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     'path': path,
                                     'overwrite': overwrite
                                 })
        if response.status_code == 200:
            return response.json()['handle']
        else:
            print(f'[dbfs.create] status: {response.status_code},\ntext: {response.text}')
            return None

    def addBlock(self, handle, data):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/dbfs/add-block'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     'handle': handle,
                                     'data': b64encode(data).decode()
                                 })
        if response.status_code == 200:
            return True
        else:
            print(f'[dbfs.addBlock] status: {response.status_code},\ntext: {response.text}')
            return False

    def close(self, handle):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/dbfs/close'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     'handle': handle
                                 })
        if response.status_code == 200:
            return True
        else:
            print(f'[dbfs.close] status: {response.status_code},\ntext: {response.text}')
            return False


    def put(self, source_path, target_path):
        """ Copy local file to remote DBFS """
        handle = self.create(target_path)
        if handle:
            try:
                with open(source_path, 'rb') as infile:
                    block = infile.read(1000 * 1000)
                    while block:
                        self.addBlock(handle, block)
                        block = infile.read(1000 * 1000)
                return True
            finally:
                self.close(handle)
        return False


    def mkdirs(self, path):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/dbfs/mkdirs'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     'path': path
                                 })
        if response.status_code == 200:
            return True
        else:
            print(f'[dbfs.mkdirs] status: {response.status_code},\ntext: {response.text}')
            return False

    def list(self, path):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/dbfs/list'),
                                headers = self.provCtx.auth_headers(),
                                json = {
                                    'path': path
                                })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[dbfs.list] status: {response.status_code},\ntext: {response.text}')
            return None

    def get_status(self, path):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/dbfs/get-status'),
                                headers = self.provCtx.auth_headers(),
                                json = {
                                    'path': path
                                })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[dbfs.get-status] status: {response.status_code},\ntext: {response.text}')
            return None
