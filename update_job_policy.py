from context import ProvContext
from policies import PoliciesApi
from jobs import JobsApi

if __name__ == '__main__':
    provCtx = ProvContext.get()
    jobsApi = JobsApi(provCtx)

    res = jobsApi.list()
    for job in res['jobs']:
        job_id = job['job_id']
        print(f'====>> job_id: {job_id}')
        res = jobsApi.get(job_id)
        print('get[before]:', res)
        # Only update if the job use an ephemeral job cluster
        if 'new_cluster' in res['settings'].keys():
            new_cluster_config = res['settings']['new_cluster']
            # Add the cluster policy id to the new config
            new_cluster_config['policy_id'] = "D0610CAEDF000004"
            res = jobsApi.update_policy(job_id, new_cluster_config)
            print('update_policy:', res)
            if res is None:
                print(f'job_id: {job_id}, update_policy [FAILED]: {res}')
            else:
                res = jobsApi.get(job_id)
                print('get[after]:', res)
