import requests


class JobsApi:
    def __init__(self, provCtx):
        self.provCtx = provCtx

    def create(self, job_config):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/jobs/create'),
                                 headers = self.provCtx.auth_headers(),
                                 json = job_config)
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[jobs.create] status: {response.status_code},\ntext: {response.text}')
            return None

    def get(self, job_id):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/jobs/get'),
                                headers = self.provCtx.auth_headers(),
                                params={'job_id': job_id})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[jobs.list] status: {response.status_code},\ntext: {response.text}')
            return None

    def update_policy(self, job_id, job_config):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/jobs/update'),
                                headers = self.provCtx.auth_headers(),
                                json={'job_id': job_id,
                                      'new_settings': job_config})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[jobs.list] status: {response.status_code},\ntext: {response.text}')
            return None

    def list(self):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/jobs/list'),
                                headers = self.provCtx.auth_headers())
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[jobs.list] status: {response.status_code},\ntext: {response.text}')
            return None

    def delete(self, job_id):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/jobs/delete'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     'job_id': job_id
                                 })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[jobs.delete] status: {response.status_code},\ntext: {response.text}')
            return None

    def reset(self, job_id, job_config):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/jobs/reset'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     'job_id': job_id,
                                     'new_settings': job_config
                                 })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[jobs.reset] status: {response.status_code},\ntext: {response.text}')
            return None

    def run_now(self, job_id):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/jobs/run-now'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     'job_id': job_id
                                 })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[jobs.run-now] status: {response.status_code},\ntext: {response.text}')
            return None

    def runs_list(self, job_id, active_only=True, offset=0, limit=20):
        if job_id is not None:
            response = requests.get(self.provCtx.api_endpoint('api/2.0/jobs/runs/list'),
                                    headers = self.provCtx.auth_headers(),
                                    params = {
                                        'job_id': job_id,
                                        'active_only': True,
                                        'completed_only': False,
                                        'offset': 0,
                                        'limit': limit
                                    })
            if response.status_code == 200:
                return response.json()
            else:
                print(f'[jobs.runs.list] status: {response.status_code},\ntext: {response.text}')
                return None
        else:
            return None

    def runs_cancel(self, run_id):
        if run_id is not None:
            response = requests.post(self.provCtx.api_endpoint('api/2.0/jobs/runs/cancel'),
                                    headers = self.provCtx.auth_headers(),
                                    json = {
                                        'run_id': run_id,
                                    })
            if response.status_code == 200:
                return True
            else:
                print(f'[jobs.runs.cancel] status: {response.status_code},\ntext: {response.text}')
                return False
        else:
            return False

    def runs_get(self, run_id):
        if run_id is not None:
            response = requests.get(self.provCtx.api_endpoint('api/2.0/jobs/runs/get'),
                                    headers = self.provCtx.auth_headers(),
                                    params = {
                                        'run_id': run_id,
                                    })
            if response.status_code == 200:
                return response.json()
            else:
                print(f'[jobs.runs.get] status: {response.status_code},\ntext: {response.text}')
                return None
        else:
            return None
