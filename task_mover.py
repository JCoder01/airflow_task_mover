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
from sqlalchemy import update, insert, select, literal_column, delete
from airflow.utils.db import provide_session
from airflow.operators.python import PythonOperator
import pendulum

target_models = {x.__tablename__: x for x in
                 [Log,
                  SlaMiss,
                  TaskReschedule,
                  XCom,
                  RenderedTaskInstanceFields,
                  TaskFail,
                  TaskMap,
                  TaskOutletDatasetReference]}

@provide_session
def ti_copy(dag_run, ti, session):
    log = ti.log
    dr_conf = dag_run.conf

    source_task_id = dr_conf['source_task_id']
    source_dag_id = dr_conf['source_dag_id']
    target_task_id = dr_conf.get('target_task_id') or source_task_id
    target_dag_id = dr_conf.get('target_dag_id') or source_dag_id
    log.info(f"copying rows: {source_dag_id}.{source_task_id} -> {target_dag_id}.{target_task_id}")
    select_columns = [literal_column(f"'{target_task_id}'").label('task_id'),
                      literal_column(f"'{target_dag_id}'").label('dag_id')
                      ]  + [x for x in TaskInstance.__table__.columns
                            if x.name not in ('task_id', 'dag_id')]


    insert_ = (insert(TaskInstance)
               .from_select(
        TaskInstance.__table__.columns,
        select(select_columns)
        .where(TaskInstance.task_id == source_task_id,
               TaskInstance.dag_id == source_dag_id)
    )
               )
    session.execute(insert_)
    session.commit()


@provide_session
def update_model(task, dag_run, ti, session):
    log = ti.log
    target_model = target_models[task.task_id]
    dr_conf = dag_run.conf

    source_task_id = dr_conf['source_task_id']
    source_dag_id = dr_conf['source_dag_id']
    target_task_id = dr_conf.get('target_task_id') or source_task_id
    target_dag_id = dr_conf.get('target_dag_id') or source_dag_id

    update_ = (update(target_model)
    .where(target_model.task_id==source_task_id,
           target_model.dag_id==source_dag_id)
    .values(task_id=target_task_id,
            dag_id=target_dag_id)
    )

    log.info(f"updating: {source_dag_id}.{source_task_id} -> {target_dag_id}.{target_task_id}")
    session.execute(update_)

    session.commit()

@provide_session
def ti_delete(dag_run, ti, session):
    log = ti.log
    dr_conf = dag_run.conf

    source_task_id = dr_conf['source_task_id']
    source_dag_id = dr_conf['source_dag_id']
    log.info(f"deleting rows: {source_dag_id}.{source_task_id}")
    delete_ = (delete(
        TaskInstance
    )    .where(TaskInstance.task_id==source_task_id,
           TaskInstance.dag_id==source_dag_id)
               )

    session.execute(delete_)
    session.commit()

@provide_session
def move_logs(task, dag_run, ti, session):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    dr_conf = dag_run.conf
    source_task_id = dr_conf['source_task_id']
    source_dag_id = dr_conf['source_dag_id']
    target_task_id = dr_conf.get('target_task_id') or source_task_id
    target_dag_id = dr_conf.get('target_dag_id') or source_dag_id

    remote_log_folder = conf.get_mandatory_value('logging', 'REMOTE_BASE_LOG_FOLDER')
    conn = conf.get('logging', 'remote_log_conn_id')
    hook = S3Hook(aws_conn_id=conn)
    bucket, prefix = hook.parse_s3_url(remote_log_folder)

    dag_keys = hook.list_keys(bucket_name=bucket,
                   prefix=f'{prefix + "/" if prefix else ""}dag_id={target_dag_id}')

    old_task_logs = [k for k in dag_keys if f'task_id={source_task_id}/]' in k]

    for s3_key in old_task_logs:
        dest_key = (s3_key
                    .replace(f'dag_id={source_dag_id}/', f'dag_id={target_dag_id}/')
                    .replace(f'task_id={source_task_id}/', f'task_id={target_task_id}/')
                    )

        hook.copy_object(source_bucket_key=s3_key, dest_bucket_key=dest_key,
                         source_bucket_name=bucket, dest_bucket_name=bucket
                         )
    hook.delete_objects(bucket, old_task_logs)


with DAG(dag_id='task_renamer',
         schedule=None,
         params={"source_task_id": None,
                 "source_dag_id": None,
                 "target_task_id": None,
                 "target_dag_id": None},
         start_date=pendulum.parse('2022-11-01')) as dag:
    # most tables have a foreign key relation to task_instance,
    # so first copy, then do updates on other tables and finally, delete from task_instance
    copy_it = PythonOperator(
        task_id='copy_source_task_instances',
        python_callable=ti_copy
    )

    updates = [
        PythonOperator(task_id=f"update_{target}",
                       python_callable=update_model)
        for target in target_models
    ]

    delete_old_it = PythonOperator(
        task_id='delete_source_task_instances',
        python_callable=ti_delete
    )
    copy_it >> updates >> delete_old_it

    if conf.getboolean('logging', 'remote_logging'):
        remote_log_folder = conf.get_mandatory_value('logging', 'REMOTE_BASE_LOG_FOLDER')
        if remote_log_folder.startswith('s3://'):
            PythonOperator(task_id='move_remote_logs_s3',
                           python_callable=move_logs,
                           )

        else:
            raise NotImplementedError

