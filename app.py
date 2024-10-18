import os
import pandas as pd
import io
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask
from dotenv import load_dotenv
from flask import Flask, request, render_template, redirect, url_for, flash
from databricks.sdk.service import catalog


load_dotenv()


app = Flask(__name__)
app.secret_key = os.getenv('APP_SECRET_KEY')

w = WorkspaceClient(
  host=os.getenv('DATABRICKS_HOST'),
  token=os.getenv('DATABRICKS_ACCESS_TOKEN')
)

volume_catalog = 'analysis'
volume_schema = 'bronze'
volume_name = 'files_folder'

notebook = '/Workspace/Repos/PA/Databricks/analysis/create_table_form_csv_in_volumes'


@app.route('/')
def upload_form():
    return render_template('upload.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)

    file = request.files['file']

    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)

    if file and file.filename.endswith('.csv'):
        try:

            df = pd.read_csv(file)
            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            try:
                loaded_volume = w.volumes.read(name=f"{volume_catalog}.{volume_schema}.{volume_name}")
            except Exception as e:
                print('Creating the volume')
                w.volumes.create(
                    catalog_name=volume_catalog, schema_name=volume_schema,
                    name=volume_name, volume_type=catalog.VolumeType.MANAGED
                )

            file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{file.filename}"
            w.files.upload(file_path=file_path_in_volume, contents=csv_buffer, overwrite=True)

            jobid = w.jobs.create(
                name='create_table_form_csv_in_volumes',
                tasks=[
                    Task(
                        existing_cluster_id=os.getenv('CLUSTER_ID'),
                        notebook_task=NotebookTask(
                            base_parameters=dict(""),
                            notebook_path=notebook
                        ),
                        task_key='create_table_form_csv_in_volumes'
                    )
                ]

            )

            notebook_params = dict(file_name=file.filename)

            run_by_id = w.jobs.run_now(job_id=jobid.job_id, notebook_params=notebook_params).result()

            flash('CSV uploaded and data inserted successfully!')

            # cleanup
            w.jobs.delete(job_id=jobid.job_id)
            return redirect(url_for('upload_form'))

        except Exception as e:
            flash(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form'))

    else:
        flash('Invalid file format. Please upload a CSV.')
        return redirect(url_for('upload_form'))


if __name__ == '__main__':
    app.run()
