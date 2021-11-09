import os

class ProvContext:
    def __init__(self, domain, access_token, management_access_token):
        self.domain = domain
        self.access_token = access_token
        self.management_access_token = management_access_token

    def api_endpoint(self, service_endpoint):
        if self.domain.startswith('https:'):
            return f'{self.domain}/{service_endpoint}'
        else:
            return f'https://{self.domain}/{service_endpoint}'

    def auth_headers(self):
        return {'Authorization': f'Bearer {self.access_token}',
                'X-Databricks-Azure-SP-Management-Token': self.management_access_token
                }

    @classmethod
    def get(cls):
        domain = os.environ['WORKSPACE_URL']
        token = os.environ['API_TOKEN']
        return ProvContext(domain, token, token)
