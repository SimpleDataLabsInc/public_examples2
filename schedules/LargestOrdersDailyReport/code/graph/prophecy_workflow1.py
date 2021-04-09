
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.models import BaseOperator


def prophecy_workflow1(config) -> BaseOperator:
    workflow_id = "381"
    workflow_version = "latest"
    return DatabricksSubmitRunOperator(task_id='prophecy_workflow1',
                                       new_cluster=config.fabric['job_sizes'][config.job_size],
                                       spark_jar_task={
                                           'main_class_name': 'Main',
                                           'parameters': ['-C' ,  'fabricName=' + (config.fabric['name'])]
                                       },
                                       databricks_conn_id = config.connId,
                                       libraries=[
                                           {
                                               'jar': 'dbfs:/FileStore/jars/prophecy/management/app/dp/%s/%s/workflow.jar' % (
                                                   workflow_id, workflow_version)
                                           },
                                           {
                                               'jar': 'dbfs:/FileStore/jars/prophecy/management/app/dp/prophecy-libs/a9ca779efa7418f84186228725e35b0063acf006/prophecy-libs.jar'
                                           }
                                       ]
                                       )

