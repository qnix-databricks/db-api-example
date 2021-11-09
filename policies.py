import json
import requests

from context import ProvContext


class PoliciesApi:
    def __init__(self, provCtx):
        self.provCtx = provCtx

    def get(self, policy_id):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/policies/clusters/get'),
                                headers = self.provCtx.auth_headers(),
                                params = {"policy_id": policy_id})
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[policies.get] status: {response.status_code},\ntext: {response.text}')
            return None

    def list(self):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/policies/clusters/list'),
                                headers = self.provCtx.auth_headers(),
                                params = {
                                    "sort_order": "DESC",
                                    "sort_column": "POLICY_CREATION_TIME"
                                })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[policies.get] status: {response.status_code},\ntext: {response.text}')
            return None

    def create(self, name, definition):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/policies/clusters/create'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                    "name": name,
                                    "definition": definition
                                })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[policies.create] status: {response.status_code},\ntext: {response.text}')
            return None

    def edit(self, policy_id, name, definition):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/policies/clusters/edit'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     "policy_id": policy_id,
                                     "name": name,
                                     "definition": definition
                                 })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[policies.edit] status: {response.status_code},\ntext: {response.text}')
            return None

    def delete(self, policy_id):
        response = requests.post(self.provCtx.api_endpoint('api/2.0/policies/clusters/delete'),
                                 headers = self.provCtx.auth_headers(),
                                 json = {
                                     "policy_id": policy_id,
                                 })
        if response.status_code == 200:
            return response.json()
        else:
            print(f'[policies.edit] status: {response.status_code},\ntext: {response.text}')
            return None



if __name__ == '__main__':
    provCtx = ProvContext.get()
    policiesApi = PoliciesApi(provCtx)

    ## List existing cluster policies in the workspace
    # res = policiesApi.list()
    # print(res)
    # for pol in res['policies']:
    #     print(pol)
    #     res = policiesApi.get(pol['policy_id'])
    #     print(f"==>> policy {pol['policy_id']}:\n{res}")


    ## Define cluster policy
    policy = {
        #### Chose one: Single-node, or Fixed-cluster, or Autoscale cluster
        ## Single-node cluster
        "num_workers": {"type": "fixed", "value": 0, "hidden": True},
        "spark_conf.spark.master": {"type": "fixed","value": "local[*]","hidden": True},
        "spark_conf.spark.databricks.cluster.profile": {"type": "fixed","value": "singleNode","hidden": True},
        "custom_tags.ResourceClass": {"type": "fixed", "value": "SingleNode","hidden": True},

        ## Fixed-cluster
        # "num_workers": {"type": "fixed", "value": 2, "hidden": True},

        ## Autoscale cluster
        # "autoscale.min_workers": {"type": "fixed","value": 2,"hidden": True},
        # "autoscale.max_workers": {"type": "fixed","value": 8,"hidden": True},

        # "spark_version": {
        #     "type": "fixed",
        #     "value": "8.4.x-scala2.12",
        #     "hidden": True
        # },
        "spark_version": {
            "type": "allowlist",
            "values": ["8.1.x-scala2.12", "8.2.x-scala2.12", "8.3.x-scala2.12", "8.4.x-scala2.12"],
            "defaultValue": "8.3.x-scala2.12"
        },

        "autotermination_minutes": {
            "type": "range",
            "minValue": 15,
            "maxValue": 90,
            "defaultValue": 30,
            "hidden": False
        },

        # Define an environment variable named `environment_name`
        "spark_env_vars.environment_name": {"type": "fixed", "value": "DEV"},

        ## Control what type of cluster can be created from this policy profile
        # "cluster_type": {"type": "fixed", "value": "job", "hidden": True},
        "cluster_type": {"type": "fixed", "value": "all-purpose", "hidden": True},

        ## Control what type of work load can be run on the cluster.
        "workload_type.clients.jobs": {"type": "fixed", "value": True, "hidden": True},
        "workload_type.clients.notebooks": {"type": "fixed", "value": True, "hidden": True}
    }

    print(f'==>> cluster policy:\n{json.dumps(policy)}')

    ## Create once
    # res = policiesApi.create("QTA TEST POLICY", json.dumps(policy))
    # print(res)

    ## ... And Edit as many time as you like -- NOTE: update with the policy_id returned by `create` call
    res = policiesApi.edit('7160DD08A800002E', "QTA TEST POLICY", json.dumps(policy))
    print(res)
