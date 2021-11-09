import sys
from context import ProvContext
from jobs import JobsApi
from security import GroupsApi
from utils import Utils

if __name__ == '__main__':
    if len(sys.argv) > 2:
        # The job_name and config_file should be provided as input arguments
        assert(len(sys.argv) > 2)

        provCtx = ProvContext.get()
        utils = Utils(provCtx)

        job_name = sys.argv[1]
        config_file = sys.argv[2]

        utils.deploy_job(job_name, config_file)
    else:
        provCtx = ProvContext.get()
        jobsApi = JobsApi(provCtx)
        # res = jobsApi.runs_list(36553)
        # print(res)

        groupsApi = GroupsApi(provCtx)
        res = groupsApi.list()
        print(res)
        res = groupsApi.list_members("admins")
        print(res)
        res = groupsApi.list_parents("quan.ta@databricks.com")
        print(res)

        res = groupsApi.create("AAA_Group")
        print('create:', res)
        res = groupsApi.add_member("quan.ta@databricks.com", "AAA_Group")
        print('add_member:', res)
        res = groupsApi.remove_member("quan.ta@databricks.com", "AAA_Group")
        print('remove_member:', res)
        res = groupsApi.delete("AAA_Group")
        print('delete:', res)
