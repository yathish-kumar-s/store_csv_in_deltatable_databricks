import os
import pandas as pd
import io
import logging
from databricks.sdk import WorkspaceClient
# from databricks.sdk.runtime.dbutils_stub import dbutils
from databricks.sdk.service.jobs import Task, NotebookTask
from dotenv import load_dotenv
from flask import Flask, request, render_template, redirect, url_for, flash

from utils import read_csv_file

load_dotenv()

environment = 'prd'
app = Flask(__name__)
app.secret_key = os.getenv('APP_SECRET_KEY')


logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
)

logger = logging.getLogger(__name__)

# application_id = dbutils.secrets.get(
#     scope=f"padss{environment}001", key=f"svcDatabricksPrd-clientid"
# )
# service_credential = dbutils.secrets.get(
#     scope=f"padss{environment}001", key=f"svcDatabricksPrd-clientsecret"
# )


# w = WorkspaceClient(
#   host=os.getenv('DATABRICKS_HOST'),
#     azure_client_id=application_id,
#     azure_client_secret=service_credential
# )


w = WorkspaceClient(
  auth_type='pat',
  host=os.getenv('DATABRICKS_HOST'),
  token=os.getenv('DATABRICKS_ACCESS_TOKEN')
)


volume_catalog = 'uut'
volume_schema = 'dbo'
volume_name = 'vol_fileuploadutility'

notebook = '/Workspace/Repos/PA/Databricks/analysis/create_table_form_csv_in_volumes'



@app.route('/')
def homepage():
    return render_template('homepage.html')

@app.route('/upload_form')
def upload_form():
    return render_template('upload.html')

@app.route('/upload_form_part_term')
def upload_form_part_term():
    return render_template('upload_part_term.html')

@app.route('/upload_form_cust_part_attr')
def upload_form_cust_part_attr():
    return render_template('upload_custom_part_attributes.html')


@app.route('/upload_csv', methods=['POST'])
def upload_file():
    volume_folder = 'apps/goodbetterbest'
    notebook_path = '/Workspace/Repos/PA/Databricks/Databricks-Apps/upload-file-notebooks/good_better_best'
    required_columns = ['lc', 'part', 'quality_id', 'quality']
    nullable_columns = ['quality_id']
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)

    file = request.files['file']

    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)

    if file and file.filename.endswith('.csv'):
        try:

            csv_buffer = read_csv_file(file, required_columns, nullable_columns)

            # try:
            #     loaded_volume = w.volumes.read(name=f"{volume_catalog}.{volume_schema}.{volume_name}/{volume_folder}")
            # except Exception as e:
            #     print('Volume does not exist')
            #     raise

            file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_folder}/{file.filename}"
            w.files.upload(file_path=file_path_in_volume, contents=csv_buffer, overwrite=True)

            jobid = w.jobs.create(
                name='create_table_form_csv_in_volumes',
                tasks=[
                    Task(
                        existing_cluster_id=os.getenv('CLUSTER_ID'),
                        notebook_task=NotebookTask(
                            base_parameters=dict(""),
                            notebook_path=notebook_path
                        ),
                        task_key='create_table_form_csv_in_volumes'
                    )
                ]

            )

            notebook_params = dict(file_name=file.filename)

            run_by_id = w.jobs.run_now(job_id=jobid.job_id, notebook_params=notebook_params).result()

            run_results = w.jobs.get_run_output(run_id=run_by_id.tasks[0].run_id).notebook_output.result

            if run_results and 'Error' in run_results:
                raise Exception(run_results)

            flash('CSV uploaded and data inserted successfully!')

            # cleanup
            w.jobs.delete(job_id=jobid.job_id)
            return redirect(url_for('upload_form'))

        except Exception as e:
            flash(f'An error occurred: Please try again')
            print(str(e))
            logger.exception(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form'))

    else:
        flash('Invalid file format. Please upload a CSV.')
        return redirect(url_for('upload_form'))


@app.route('/upload_part_term', methods=['POST'])
def upload_part_term():
    volume_folder = 'apps/part_term'
    notebook_path = '/Workspace/Repos/PA/Databricks/Databricks-Apps/upload-file-notebooks/part_term'
    required_columns = ['lc', 'part', 'parttermid']
    nullable_columns = []

    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)

    file = request.files['file']

    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)

    if file and file.filename.endswith('.csv'):
        try:

            csv_buffer = read_csv_file(file, required_columns, nullable_columns)

            try:
                loaded_volume = w.volumes.read(name=f"{volume_catalog}.{volume_schema}.{volume_name}/{volume_folder}")
            except Exception as e:
                print('Volume does not exist')

            file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_folder}/{file.filename}"
            w.files.upload(file_path=file_path_in_volume, contents=csv_buffer, overwrite=True)

            jobid = w.jobs.create(
                name='create_table_part_terms_from_csv_in_volumes',
                tasks=[
                    Task(
                        existing_cluster_id=os.getenv('CLUSTER_ID'),
                        notebook_task=NotebookTask(
                            base_parameters=dict(""),
                            notebook_path=notebook_path
                        ),
                        task_key='create_table_part_terms_from_csv_in_volumes'
                    )
                ]

            )

            notebook_params = dict(file_name=file.filename)

            run_by_id = w.jobs.run_now(job_id=jobid.job_id, notebook_params=notebook_params).result()

            run_results = w.jobs.get_run_output(run_id=run_by_id.tasks[0].run_id).notebook_output.result

            if run_results and 'Error' in run_results:
                raise Exception(run_results)

            flash('CSV uploaded and data inserted successfully!')

            # cleanup
            w.jobs.delete(job_id=jobid.job_id)
            return redirect(url_for('upload_form_part_term'))

        except Exception as e:
            flash(f'An error occurred: Please try again')
            logger.exception(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form_part_term'))

    else:
        flash('Invalid file format. Please upload a CSV.')
        return redirect(url_for('upload_form_part_term'))


@app.route('/upload_custom_part_attributes', methods=['POST'])
def upload_custom_part_attributes():
    volume_folder = 'apps/custompartattributes'
    notebook_path = '/Workspace/Repos/PA/Databricks/Databricks-Apps/upload-file-notebooks/custompartattributes'
    required_columns = ['lc', 'part',  'partattributecode', 'partattributedescription']
    nullable_columns = ['partattributecode']
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)

    file = request.files['file']

    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)

    if file and file.filename.endswith('.csv'):
        try:
            csv_buffer = read_csv_file(file, required_columns, nullable_columns)

            try:
                loaded_volume = w.volumes.read(name=f"{volume_catalog}.{volume_schema}.{volume_name}/{volume_folder}")
            except Exception as e:
                print('Volume does not exist')

            file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_folder}/{file.filename}"
            w.files.upload(file_path=file_path_in_volume, contents=csv_buffer, overwrite=True)

            jobid = w.jobs.create(
                name='create_table_form_csv_in_volumes',
                tasks=[
                    Task(
                        existing_cluster_id=os.getenv('CLUSTER_ID'),
                        notebook_task=NotebookTask(
                            base_parameters=dict(""),
                            notebook_path=notebook_path
                        ),
                        task_key='create_table_form_csv_in_volumes'
                    )
                ]

            )

            notebook_params = dict(file_name=file.filename)

            run_by_id = w.jobs.run_now(job_id=jobid.job_id, notebook_params=notebook_params).result()

            run_results = w.jobs.get_run_output(run_id=run_by_id.tasks[0].run_id).notebook_output.result

            if run_results and 'Error' in run_results:
                raise Exception(run_results)

            flash('CSV uploaded and data inserted successfully!')

            # cleanup
            w.jobs.delete(job_id=jobid.job_id)
            return redirect(url_for('upload_form_cust_part_attr'))

        except Exception as e:
            flash(f'An error occurred: Please try again')
            logger.exception(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form_cust_part_attr'))

    else:
        flash('Invalid file format. Please upload a CSV.')
        return redirect(url_for('upload_form_cust_part_attr'))


if __name__ == '__main__':
    app.run()
