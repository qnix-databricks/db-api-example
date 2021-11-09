import requests

from context import ProvContext


class UsersApi:
    def __init__(self, provCtx):
        self.provCtx = provCtx

    def me(self):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/preview/scim/v2/Me'),
                                headers = self.provCtx.auth_headers())
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[users.me] status: {response.status_code},\ntext: {response.text}')
            return None

    def users(self):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/preview/scim/v2/Users'),
                                headers = self.provCtx.auth_headers())
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[users.me] status: {response.status_code},\ntext: {response.text}')
            return None


if __name__ == '__main__':
    provCtx = ProvContext.get()
    usersApi = UsersApi(provCtx)

    ## List all the users in the workspace
    res = usersApi.users()
    # print(res)
    for u in res['Resources']:
        try:
            print('==>> User:', u['displayName'], '\n',
                'id:', u['id'], ', userName', u['userName'], '\n',
                'entitlements:', u['entitlements'], '\n',
                )
        except:
            pass
