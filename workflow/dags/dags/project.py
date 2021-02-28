try:
    import os
    import sys

    from datetime import timedelta,datetime
    from airflow import DAG

    # Operators
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.email_operator import EmailOperator
    from airflow.utils.trigger_rule import TriggerRule

    from airflow.utils.task_group import TaskGroup
    from common.awshelper import AWSS3
    import pandas as pd

    print("All Dag modules are ok ......")

except Exception as e:
    print("Error  {} ".format(e))


# ===============================================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'email': ['shahsoumil519@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}
dag = DAG(dag_id="project", schedule_interval="@once", default_args=default_args, catchup=False)
# ================================================


def crawl_files(**context):
    path = os.path.join(os.getcwd(), "dags/common")
    os.chdir(path)

    files = []
    for x in os.listdir():
        if ".csv" in x:
            files.append(x)

    context['ti'].xcom_push(key='files', value=files)


def upload_s3(**context):

    """Upload on AWS S3 """

    files = context.get("ti").xcom_pull(key="files")
    aws_helper  = AWSS3()

    try:
        path = os.path.join(os.getcwd(), "dags/common")
        os.chdir(path)
        print("Good")
    except Exception as e:
        print('Error : {} '.format(e))

    for file in files:
        with open(file, "rb") as f:
            try:
                data = f.read()
                bucket_path = "soumil/{}".format(file)
                aws_helper.put_files(Key=bucket_path, Body=data)
                print("File : {} Uploaded ".format(file))
            except Exception as e:
                print("Failed to upload File :{} ".format(e))


def trigger_glue(**context):
    pass


with DAG(dag_id="project", schedule_interval="@once", default_args=default_args, catchup=False) as dag:

    crawl_files = PythonOperator(task_id="crawl_files",python_callable=crawl_files,provide_context=True,)
    upload_s3 = PythonOperator(task_id="upload_s3",python_callable=upload_s3,provide_context=True,)
    trigger_glue = PythonOperator(task_id="trigger_glue",python_callable=trigger_glue,provide_context=True,)

    email = EmailOperator(
        task_id='send_email',
        to='XXXXXXXXXXXXXXX',
        subject='Airflow Alert',
        html_content=""" <h3>ETL Pipeline complete </h3> """,
    )

crawl_files >> upload_s3 >> trigger_glue >> email

