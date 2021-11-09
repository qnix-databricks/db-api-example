import requests

class GroupsApi:
    def __init__(self, provCtx):
        self.provCtx = provCtx

    def list(self):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/groups/list'),
                                headers = self.provCtx.auth_headers())
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[groups.list] status: {response.status_code},\ntext: {response.text}')
            return None

    def list_members(self, group):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/groups/list-members'),
                                headers = self.provCtx.auth_headers(),
                                params = {"group_name": group})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[groups.list-members] status: {response.status_code},\ntext: {response.text}')
            return None

    def list_parents(self, entity):
        """ entity: user or group """
        response = requests.get(self.provCtx.api_endpoint('api/2.0/groups/list-parents'),
                                headers = self.provCtx.auth_headers(),
                                params = {"user_name": entity})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[groups.list-parents] status: {response.status_code},\ntext: {response.text}')
            return None

    def create(self, group_name):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/groups/create'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {"group_name": group_name})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[groups.list-members] status: {response.status_code},\ntext: {response.text}')
            return None

    def add_member(self, user_name, group_name):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/groups/add-member'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     "user_name": user_name,
                                     "parent_name": group_name
                                 })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[groups.add-member] status: {response.status_code},\ntext: {response.text}')
            return None

    def remove_member(self, user_name, group_name):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/groups/remove-member'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     "user_name": user_name,
                                     "parent_name": group_name
                                 })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[groups.remove-member] status: {response.status_code},\ntext: {response.text}')
            return None

    def delete(self, group_name):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/groups/delete'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {"group_name": group_name})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[groups.delete] status: {response.status_code},\ntext: {response.text}')
            return None
