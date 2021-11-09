from base64 import b64encode
import os.path as op
import requests
import time

from context import ProvContext


class ClustersApi:
    def __init__(self, provCtx):
        self.provCtx = provCtx

    def create(self, job_config_json):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/clusters/create'),
                                 headers = self.provCtx.auth_headers(),
                                 json = job_config_json)
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.create] status: {response.status_code},\ntext: {response.text}')
            return None

    def edit(self, job_config_json):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/clusters/edit'),
                                 headers = self.provCtx.auth_headers(),
                                 json = job_config_json)
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.edit] status: {response.status_code},\ntext: {response.text}')
            return None

    def get(self, cluster_id):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/clusters/get'),
                                headers = self.provCtx.auth_headers(),
                                params = {"cluster_id": cluster_id})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.get] status: {response.status_code},\ntext: {response.text}')
            return None

    def terminate(self, cluster_id):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/clusters/delete'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {"cluster_id": cluster_id})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.terminate] status: {response.status_code},\ntext: {response.text}')
            return None

    def delete(self, cluster_id):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/clusters/permanent-delete'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {"cluster_id": cluster_id})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.delete] status: {response.status_code},\ntext: {response.text}')
            return None

    def start(self, cluster_id):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/clusters/start'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {"cluster_id": cluster_id})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.start] status: {response.status_code},\ntext: {response.text}')
            return None

    def restart(self, cluster_id):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/clusters/restart'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {"cluster_id": cluster_id})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.restart] status: {response.status_code},\ntext: {response.text}')
            return None

    def resize(self, cluster_id, num_workers):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/clusters/resize'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     "cluster_id": cluster_id,
                                     "num_workers": num_workers
                                 })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.resize] status: {response.status_code},\ntext: {response.text}')
            return None

    def pin(self, cluster_id):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/clusters/pin'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {"cluster_id": cluster_id})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.pin] status: {response.status_code},\ntext: {response.text}')
            return None

    def unpin(self, cluster_id):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/clusters/unpin'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {"cluster_id": cluster_id})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.unpin] status: {response.status_code},\ntext: {response.text}')
            return None

    def list(self):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/clusters/list'),
                                headers = self.provCtx.auth_headers())
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[clusters.list] status: {response.status_code},\ntext: {response.text}')
            return None

    def wait(self, cluster_id):
        """ Certain API operation required the cluster is either in RUNNING, or TERMINATED state.
            This function just wait until the cluster is in the one of these states.
        """
        editable_states = set(['RUNNING', 'TERMINATED'])
        res = self.get(cluster_id)
        print('=> wait get:', res)
        while res['state'] not in editable_states:
            time.sleep(15)
            res = self.get(cluster_id)
            print('=> wait get:', res)
        print('==>> Current cluster state:', res['state'])

if __name__ == '__main__':
    provCtx = ProvContext.get()
    clustersApi = ClustersApi(provCtx)

    cluster_config = {
        "cluster_name": "my-cluster",
        "spark_version": "8.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "autotermination_minutes": 20,
        "policy_id": "7160DD08A800002E"
    }
    res = clustersApi.create(cluster_config)
    print('create:', res)
    cluster_id = res['cluster_id']

    # clustersApi.wait(cluster_id)

    # edit_cluster_config = {
    #     "cluster_id": cluster_id,
    #     "cluster_name": "my-cluster",
    #     "spark_version": "8.4.x-scala2.12",
    #     "node_type_id": "i3.xlarge",
    #     "num_workers": 4
    # }
    # res = clustersApi.edit(edit_cluster_config)
    # print('create:', res)

    # res = clustersApi.terminate(cluster_id)
    # res = clustersApi.start(cluster_id)

    # clustersApi.wait(cluster_id)

    # res = clustersApi.restart(cluster_id)

    # clustersApi.wait(cluster_id)

    # res = clustersApi.resize(cluster_id, 8)

    # clustersApi.wait(cluster_id)

    # clustersApi.delete(cluster_id)

    # res = clustersApi.list()
    # for cls in res['clusters']:
    #     if cls['creator_user_name'] == 'quan.ta@databricks.com':
    #         print('==>> cluster:', cls)
    #         clustersApi.delete(cls['cluster_id'])
