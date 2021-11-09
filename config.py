
class ClusterConfig:
    def __init__(self, cluster_settings):
        self.config = cluster_settings

    def settings(self):
        return self.config

    def update_config(self, job_settings):
        assert(job_settings)
        if 'num_workers' in job_settings.keys():
            self.set_num_workers(job_settings['num_workers'])

        if 'instance_pool_id' in job_settings.keys():
            self.add_instance_pool(job_settings['instance_pool_id'])

        if 'init_script' in job_settings.keys():
            self.add_init_script(job_settings['init_script'])

    def set_num_workers(self, num_workers):
        assert(num_workers > 0)
        self.config['num_workers'] = num_workers

    def add_instance_pool(self, instance_pool_id):
        if instance_pool_id:
            # Cannot have both instance_pool_id and node_type_id
            if "node_type_id" in self.config.keys():
                del self.config["node_type_id"]
            self.config['instance_pool_id'] = instance_pool_id


    def add_init_script(self, init_script):
        self.config['init-scripts'].append(init_script)

    def add_spark_conf(self, sc_name, sc_value):
        self.config['spark_conf'][sc_name] = sc_value

    def add_spark_env_var(self, env_name, env_value):
        self.config['spark_env_vars'][env_name] = env_value


class JobConfig:
    def __init__(self, job_name, dbfs_jar_path, job_settings, cluster_config):
        job_main_class = job_settings['main_class']
        parameters = []
        if 'parameters' in job_settings.keys():
            parameters = job_settings['parameters']
        self.config = {
            "name": job_name,
            "libraries": [{"jar": dbfs_jar_path}],
            "spark_jar_task": {
                "main_class_name": job_main_class,
                "parameters": parameters,
                "run_as_repl": True
            },
            "email_notifications": {},
            "max_concurrent_runs": 1
        }
        # Pick up any addition configuration settings
        print(f'==>> cluster_config: {cluster_config.settings()}')
        print(f'==>> job_config: {self.config}')
        self.update_job_settings(cluster_config.settings())
        if 'job_settings' in job_settings.keys():
            self.update_job_settings(job_settings['job_settings'])
        print("AFTER")
        print(f'====>> cluster_config: {cluster_config.settings()}')
        print(f'====>> job_config: {self.config}')

    def update_job_settings(self, job_settings):
        # If value is a list, we append.  Otherwise, it's an over-write
        for k, vs in job_settings.items():
            if type(vs) == list:
                for v in vs:
                    self.config[k].append(v)
            else:
                self.config[k] = vs

    def settings(self):
        return self.config
