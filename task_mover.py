"""
tables with "task_id" column:
log
sla_miss
task_reschedule
task_instance
task_map
celery_taskmeta
xcom
rendered_task_instance_fields
task_fail
task_outlet_dataset_reference
"""

from airflow.models.log import Log
from airflow.models.slamiss import SlaMiss
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCom
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.taskfail import TaskFail
from airflow.models.taskmap import TaskMap
from airflow.models.dataset import TaskOutletDatasetReference
from airflow.models.dag import DAG, DagRun
from airflow.configuration import conf
from sqlalchemy import update
from airflow.utils.db import provide_session
from airflow.operators.python import PythonOperator
import pendulum

target_models = {x.__name__: x for x in [Log,
                 SlaMiss,
                 TaskReschedule,
                 TaskInstance,
                 XCom,
                 RenderedTaskInstanceFields,
                 TaskFail,
                 TaskMap,
                 TaskOutletDatasetReference]}

@provide_session
def update_model(target, old_name:str, new_name: str, dag_id, log, session):
    update_ = (update(target)
    .where(target.task_id==old_name, target.dag_id==dag_id)
    .values(task_id=new_name)
    )

    log.info(f"running {update_}")
    log.info(f"params: old_name {old_name}, new_name {new_name}, dag_id {dag_id}")
    session.execute(update_)

    session.commit()

def do_update(task, dag_run: DagRun, ti):
    c = dag_run.conf
    update_model(target=target_models[task.task_id], log=ti.log, **c)

@provide_session
def move_logs(task, dag_run, ti, session):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    target_dag_id = dag_run.conf['dag_id']
    source_task_id = dag_run.conf['old_name']
    target_task_id = dag_run.conf['new_name']
    remote_log_folder = conf.get_mandatory_value('logging', 'REMOTE_BASE_LOG_FOLDER')
    conn = conf.get('logging', 'remote_log_conn_id')
    hook = S3Hook(aws_conn_id=conn)
    bucket, prefix = hook.parse_s3_url(remote_log_folder)

    dag_keys = hook.list_keys(bucket_name=bucket,
                   prefix=f'{prefix + "/" if prefix else ""}dag_id={target_dag_id}')

    old_task_logs = [k for k in dag_keys if f'task_id={source_task_id}/]' in k]

    for s3_key in old_task_logs:
        dest_key = s3_key.replace(f'task_id={source_task_id}/', f'task_id={target_task_id}/')
        hook.copy_object(source_bucket_key=s3_key, dest_bucket_key=dest_key,
                         source_bucket_name=bucket, dest_bucket_name=bucket
                         )
    hook.delete_objects(bucket, old_task_logs)


with DAG(dag_id='task_renamer',
         schedule=None,
         params={"old_name": None,
                 "new_name": None,
                 "dag_id": None},
         start_date=pendulum.parse('2022-11-01')) as dag:
    for target in target_models:
        PythonOperator(task_id=target,
                       python_callable=do_update)
    if conf.getboolean('logging', 'remote_logging'):
        remote_log_folder = conf.get_mandatory_value('logging', 'REMOTE_BASE_LOG_FOLDER')
        if remote_log_folder.startswith('s3://'):
            PythonOperator(task_id='move_remote_logs_s3',
                           python_callable=move_logs,
                           )

        else:
            raise NotImplementedError
