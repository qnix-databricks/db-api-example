
import json
import os.path as op
import requests
import subprocess
import time

from jobs import JobsApi
from dbfs import DbfsApi

class Utils:
    def __init__(self, provCtx):
        self.provCtx = provCtx
        self.jobsApi = JobsApi(provCtx)
        self.dbfsApi = DbfsApi(provCtx)

    def find_file(self, find_cmd):
        p = subprocess.Popen(find_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            return p.stdout.read().decode().strip()
        except:
            raise Exception(p.stderr.read())
        finally:
            p.wait()

    def get_job_id(self, job_name, job_main_class):
        """ Find the job_id of the given the job name and main class """
        res = self.jobsApi.list()
        job_id = None
        try:
            for job in res['jobs']:
                if job['settings']['name'] == job_name and \
                job['settings']['spark_jar_task']['main_class_name'] == job_main_class:
                    job_id = job['job_id']
            return job_id
        except KeyError:
            return None

    def query_job_cluster(self, run_name):
        response = requests.get(self.provCtx.api_endpoint('api/2.0/clusters/list'),
                                headers = self.provCtx.auth_headers())
        if response.status_code == 200:
            res = response.json()
            with open("/tmp/cluster.json", "w") as outfile:
                for cluster in res['clusters']:
                    print(self.pp(cluster), file=outfile)
                    if cluster['state'] == "RUNNING" and \
                       cluster['cluster_source'] == "JOB" and \
                       cluster['default_tags']['RunName'] == run_name:
                        return cluster['cluster_id']
        else:
            print(f'[dbfs.list] status: {response.status_code},\ntext: {response.text}')
            return None

    def restart_job(self, job_id, max_wait_time=300):
        # Check if there are active run of the job_id
        res = self.jobsApi.runs_list(job_id)
        run_ids = []
        for run in res['runs']:
            if run['state']['life_cycle_state'] in {'PENDING', 'RUNNING'}:
                print('run:', self.pp(run))
                run_id = run['run_id']
                print(f'run_id {run_id} for job_id {job_id} is active. Cancel the active run.')
                self.jobsApi.runs_cancel(run_id)
                run_ids.append(run_id)

        # Wait for runs to terminate
        for run_id in run_ids:
            run = self.jobsApi.runs_get(run_id)
            print('run:', self.pp(run))
            # Guard against infinite waiting, wait at most 5 minutes
            # for a cancelled job to terminate
            wait_time = 0
            while wait_time < max_wait_time and run['state']['life_cycle_state'] not in {'TERMINATED'}:
                time.sleep(5)
                wait_time += 5
                run = self.jobsApi.runs_get(run_id)

        # Now ready to run
        self.jobsApi.run_now(job_id)

    def deploy_jar(self, project_name, dbfs_path):
        """ Deploy Jar to Databricks DBFS. Return the full DBFS path to the Jar file.
            Assume the SBT build create the Jar file in <target> directory
        """
        jar_file = self.find_file(f'find target -name {project_name}*.jar')

        self.dbfsApi.mkdirs(dbfs_path)
        jar_filename = op.basename(jar_file)
        dbfs_jar_path = f'{dbfs_path}/{jar_filename}'
        if not self.dbfsApi.put(jar_file, dbfs_jar_path):
            print(f'Failed to copy {jar_file} to DBFS')

        res = self.dbfsApi.get_status(dbfs_jar_path)
        print(res)
        assert(res['file_size'] > 0)
        return dbfs_jar_path

    def deploy_jar_job(self, job_id, job_config):
        print(f"====>> deploy_jar_job: job_config: ${job_config}")
        if job_id is None:
            # ... job not exists, create the job
            res0 = self.jobsApi.create(job_config)
            job_id = res0['job_id']
            print('jobs.create:', res0)

            # ... and run the job
            res1 = self.jobsApi.run_now(job_id)
            print('jobs.run-now:', res1)
        else:
            # ... job definition already exists, resetting the same job
            res0 = self.jobsApi.reset(job_id, job_config)
            print('jobs.reset:', res0)

            # ... and *restart* the job
            self.restart_job(job_id)

    def load_job_settings(self, job_name, config_file):
        """ Assume with Bosch, job will have unique name """
        with open(config_file) as infile:
            js = json.load(infile)
            if job_name in js['job_defs'].keys():
                job_config = js['job_defs'][job_name]
                cluster_name = job_config['cluster']
                cluster_config = js['clusters'][cluster_name]

                ##
                ## Check if reusing existing job cluster
                ##
                if 'existing_job_cluster' in cluster_config.keys():
                    run_name = cluster_config['existing_job_cluster']
                    cluster_id = self.query_job_cluster(run_name)
                    if cluster_id:
                        cluster_config = {"existing_cluster_id": cluster_id}

                return (job_config, cluster_config)
            return (None, None)

    def pp(self, js):
        return json.dumps(js, indent=4, sort_keys=True)

    def deploy_job(self, job_name, config_file):
        job_settings, cluster_settings = self.load_job_settings(job_name, config_file)
        if job_settings is None:
            raise Exception(f"Missing definition for job {job_name}")
        project_name = job_settings['project_name']
        job_main_class = job_settings['main_class']
        dbfs_path = job_settings['dbfs_path']

        # NOTE: if run on existing cluster, the cluster should be restarted!!!
        dbfs_jar_path = self.deploy_jar(project_name, dbfs_path)
        print(f"====>> init cluster settings: {self.pp(cluster_settings)}")
        print(f"====>> init job settings: {self.pp(job_settings)}")
        cluster_config = ClusterConfig(cluster_settings)

        # Set num_workers, instance_pool_id, init_script from job_settings
        cluster_config.update_config(job_settings)

        job_config = JobConfig(job_name, dbfs_jar_path, job_settings, cluster_config)

        # Check the job status if existing job
        job_id = self.get_job_id(job_name, job_main_class)

        # Deploy and restart the job
        print(f'==>> job_config: {self.pp(job_config.settings())}', )
        self.deploy_jar_job(job_id, job_config.settings())
