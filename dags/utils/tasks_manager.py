from airflow.contrib.operators import kubernetes_pod_operator
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import gcs_delete_operator
from airflow.operators import python_operator
from airflow.operators import bash_operator


class GKEConfig(object):

    def __init__(self, project_id, cluster_name, zone):
        self.cluster_name = cluster_name
        self.zone = zone
        self.project_id = project_id


class TaskFabric(object):

    def __init__(self, task_id):
        self.task_id = task_id


class CreateAdditionalPoolTaskFabric(TaskFabric):

    def __init__(self, gke_config, pool_name, machine_type, num_nodes, disk_size):
        super().__init__(self._generate_task_id(pool_name))
        self.pool_name = pool_name
        self.gke_config = gke_config
        self.machine_type = machine_type
        self.num_nodes = num_nodes
        self.disk_size = disk_size

        self._create_additional_pool_cmd = '''
            gcloud container node-pools create {{ params.POOL_NAME }} \
                --cluster {{ params.GKE_CLUSTER_NAME }} \
                --project {{ params.PROJECT_ID }} \
                --zone {{ params.GKE_ZONE }} \
                --machine-type {{ params.POOL_MACHINE_TYPE }} \
                --num-nodes {{ params.POOL_NUM_NODES }} \
                --disk-size {{ params.POOL_DISK_SIZE }} \
                --disk-type pd-ssd \
                --scopes gke-default,storage-rw,bigquery
        '''

    @staticmethod
    def _generate_task_id(pool_name):
        return "create-{}".format(pool_name.replace("_", "-"))

    def create_task(self) -> bash_operator.BashOperator:
        return bash_operator.BashOperator(task_id=self.task_id,
                                          bash_command=self._create_additional_pool_cmd,
                                          params={"POOL_NAME": self.pool_name,
                                                  "GKE_CLUSTER_NAME": self.gke_config.cluster_name,
                                                  "PROJECT_ID": self.gke_config.project_id,
                                                  "GKE_ZONE": self.gke_config.zone,
                                                  "POOL_MACHINE_TYPE": self.machine_type,
                                                  "POOL_NUM_NODES": self.num_nodes,
                                                  "POOL_DISK_SIZE": self.disk_size
                                                  })


class DeleteAdditionalPoolTaskFabric(TaskFabric):

    def __init__(self, gke_config, pool_name):
        super().__init__(self._generate_task_id(pool_name))
        self.pool_name = pool_name
        self.gke_config = gke_config

        self._delete_additional_pool_cmd = '''
            gcloud container node-pools delete {{ params.POOL_NAME }} \
                --zone {{ params.GKE_ZONE }} \
                --cluster {{ params.GKE_CLUSTER_NAME }} \
                -q
        '''

    @staticmethod
    def _generate_task_id(pool_name):
        return "delete-{}".format(pool_name.replace("_", "-"))

    def create_task(self) -> bash_operator.BashOperator:
        return bash_operator.BashOperator(task_id=self.task_id,
                                          bash_command=self._delete_additional_pool_cmd,
                                          params={"POOL_NAME": self.pool_name,
                                                  "GKE_CLUSTER_NAME": self.gke_config.cluster_name,
                                                  "PROJECT_ID": self.gke_config.project_id,
                                                  "GKE_ZONE": self.gke_config.zone
                                                  },
                                          trigger_rule="all_done")
