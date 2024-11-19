import os
import pandas as pd
import logging
from datetime import datetime
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from flask import Flask, request, render_template, redirect, url_for, flash, send_file, jsonify, make_response

from database_connector import db_connector
from utils import read_csv_file, create_templates_df_csv_buffer, validate_good_better_best, get_csv_buffer, \
    create_templates_df_cpa, create_templates_df_cpa_prefilled_sku, validate_custom_part_attributes

load_dotenv()

environment = 'prd'
app = Flask(__name__)
app.secret_key = os.getenv('APP_SECRET_KEY')

GBB_NOTEBOOK_JOB = '528538988059648'
CPA_NOTEBOOK_JOB = '55857284532359'
PT_NOTEBOOK_JOB = '440371801732973'


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
    try:
        with db_connector() as connection:
            with connection.cursor() as cursor:
                query = """
                    select TermID, TermName from(
                      select x.partterminologyid as TermID,y.partterminologyname as TermName,rnk
                      from (
                        select
                          x.partterminologyid,sum(sales) sales,
                          row_number() over (order by sum(sales) desc) rnk  
                        from (
                          select s.partterminologyid,a.sku,sum(salesextended) sales 
                          from dst.gold.demands a
                          left join dst.gold.skumaster s on a.sku = s.sku
                          group by s.partterminologyid,a.sku) x 
                          group by x.partterminologyid
                          ) x 
                        left join catalogdata.silver.static_pcdb_bi y on x.partterminologyid = y.partterminologyid
                        )
                    where TermID in (5808, 1896)
                """

                rows = cursor.execute(query).fetchall()
                # data_dict = [row.asDict() for row in rows]
                id_list = [f'{row.TermID}-{row.TermName}' for row in rows]
        return render_template('upload_custom_part_attributes.html', id_list=id_list)
    except Exception as e:
        flash(f'An error occurred: Please try again', category='error')
        logger.exception(f'An error occurred: {str(e)}')
        return redirect(url_for('homepage'))


@app.route('/upload_csv', methods=['POST'])
def upload_file():
    timestamp = datetime.now()
    volume_folder = f'apps/goodbetterbest/{timestamp.year}/{timestamp.month}'
    # notebook_path = '/Workspace/Repos/PA/Databricks/Databricks-Apps/upload-file-notebooks/good_better_best'
    required_columns = ['lc', 'part', 'good', 'better', 'best', 'ultra_premium']
    nullable_columns = ['good', 'better', 'best', 'ultra_premium']

    if 'file' not in request.files:
        flash('No file part', category='error')
        return redirect(request.url)

    file = request.files['file']
    if file.filename == '':
        flash('No selected file', category='error')
        return redirect(request.url)

    if file and file.filename.endswith('.xlsx'):
        try:
            filename = f'{file.filename[:-5]}.csv'
            df = pd.read_excel(file)
            df = validate_good_better_best(df, required_columns, nullable_columns)
            csv_buffer = get_csv_buffer(df)

            file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_folder}/{filename}"
            w.files.upload(file_path=file_path_in_volume, contents=csv_buffer, overwrite=True)

            notebook_params = dict(file_name=filename)
            run_by_id = w.jobs.run_now(job_id=GBB_NOTEBOOK_JOB, notebook_params=notebook_params).result()
            run_results = w.jobs.get_run_output(run_id=run_by_id.tasks[0].run_id).notebook_output.result

            if run_results and 'Error' in run_results:
                raise Exception(run_results)

            flash('File uploaded and data inserted successfully!', category='success')
            return redirect(url_for('upload_form'))

        except Exception as e:
            flash(f'An error occurred: Please try again', category='error')
            print(str(e))
            logger.exception(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form'))

    else:
        flash('Invalid file format. Please upload a XLSX.', category='error')
        return redirect(url_for('upload_form'))


@app.route('/upload_part_term', methods=['POST'])
def upload_part_term():

    # cluster_state = w.clusters.get(cluster_id=os.getenv('CLUSTER_ID')).state.value
    # if cluster_state != 'RUNNING':
    #     if cluster_state == 'TERMINATED':
    #         w.clusters.start(cluster_id=os.getenv('CLUSTER_ID'))
    #         flash(f"Cluster is offline. Please try again in 5 mins", category='info')
    #         return redirect(url_for('upload_form_part_term'))
    #     flash(f"Cluster is offline. Please try again in 5 mins", category='info')
    #     return redirect(url_for('upload_form_part_term'))
    timestamp = datetime.now()
    volume_folder = f'apps/part_term/{timestamp.year}/{timestamp.month}'
    # notebook_path = '/Workspace/Repos/PA/Databricks/Databricks-Apps/upload-file-notebooks/part_term'
    required_columns = ['lc', 'part', 'parttermid']
    nullable_columns = []

    if 'file' not in request.files:
        flash('No file part', category='error')
        return redirect(request.url)

    file = request.files['file']

    if file.filename == '':
        flash('No selected file', category='error')
        return redirect(request.url)

    if file and file.filename.endswith('.xlsx'):
        try:
            filename = f'{file.filename[:-5]}.csv'
            csv_buffer = read_csv_file(file, required_columns, nullable_columns)
            file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_folder}/{filename}"
            w.files.upload(file_path=file_path_in_volume, contents=csv_buffer, overwrite=True)

            # jobid = w.jobs.create(
            #     name='create_table_part_terms_from_csv_in_volumes',
            #     tasks=[
            #         Task(
            #             existing_cluster_id=os.getenv('CLUSTER_ID'),
            #             notebook_task=NotebookTask(
            #                 base_parameters=dict(""),
            #                 notebook_path=notebook_path
            #             ),
            #             task_key='create_table_part_terms_from_csv_in_volumes'
            #         )
            #     ]
            #
            # )

            notebook_params = dict(file_name=filename)
            run_by_id = w.jobs.run_now(job_id=PT_NOTEBOOK_JOB, notebook_params=notebook_params).result()
            run_results = w.jobs.get_run_output(run_id=run_by_id.tasks[0].run_id).notebook_output.result

            if run_results and 'Error' in run_results:
                raise Exception(run_results)

            flash('File uploaded and data inserted successfully!', category='success')

            # cleanup
            # w.jobs.delete(job_id=jobid.job_id)
            return redirect(url_for('upload_form_part_term'))

        except Exception as e:
            flash(f'An error occurred: Please try again', category='error')
            logger.exception(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form_part_term'))

    else:
        flash('Invalid file format. Please upload a XLSX.', category='error')
        return redirect(url_for('upload_form_part_term'))


@app.route('/upload_custom_part_attributes', methods=['POST'])
def upload_custom_part_attributes():
    timestamp = datetime.now()
    volume_folder = f'apps/custompartattributes/{timestamp.year}/{timestamp.month}'
    # notebook_path = '/Workspace/Repos/PA/Databricks/Databricks-Apps/upload-file-notebooks/custompartattributes'
    required_columns = ['lc', 'partid', 'part', 'partattributetype', 'partattributecategory']
    nullable_columns = []
    if 'file' not in request.files:
        flash('No file part', category='error')
        return redirect(request.url)

    file = request.files['file']

    if file.filename == '':
        flash('No selected file', category='error')
        return redirect(request.url)

    if file and file.filename.endswith('.xlsx'):
        try:
            filename = f'{file.filename[:-5]}.csv'

            df = pd.read_excel(file)
            df = validate_custom_part_attributes(df, required_columns, nullable_columns)
            csv_buffer = get_csv_buffer(df)
            file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_folder}/{filename}"
            w.files.upload(file_path=file_path_in_volume, contents=csv_buffer, overwrite=True)

            notebook_params = dict(file_name=filename)

            run_by_id = w.jobs.run_now(job_id=CPA_NOTEBOOK_JOB, notebook_params=notebook_params).result()

            run_results = w.jobs.get_run_output(run_id=run_by_id.tasks[0].run_id).notebook_output.result

            if run_results and 'Error' in run_results:
                raise Exception(run_results)

            flash('File uploaded and data inserted successfully!', category='success')
            return redirect(url_for('upload_form_cust_part_attr'))

        except Exception as e:
            flash(f'An error occurred: Please try again', category='error')
            logger.exception(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form_cust_part_attr'))

    else:
        flash('Invalid file format. Please upload a XLSX.', category='error')
        return redirect(url_for('upload_form_cust_part_attr'))


@app.route('/download_gbb_template', methods=['GET'])
def download_gbb_template():
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        columns = ['lc', 'part', 'good', 'better', 'best', 'ultra_premium']
        csv_stream = create_templates_df_csv_buffer(columns)

        # Send CSV file as an attachment
        return send_file(
            csv_stream,
            as_attachment=True,
            download_name=f"good_better_best_template_{timestamp}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        flash(f'An error occurred: Please try again', category='error')
        logger.exception(f'An error occurred: {str(e)}')
        print(url_for('upload_form'))
        return redirect(url_for('upload_form'))


@app.route('/download_cpa_template', methods=['GET'])
def download_cpa_template():
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        part_id = request.args.get('CpaPartId')
        cpa_type = request.args.get('cpaType')
        cpa_category = request.args.get('cpaCat')
        blank_template = request.args.get('prefill_sku')

        default_columns = ['PartId', 'LC', 'Part', 'PartAttributeType', 'PartAttributeCategory']

        with db_connector() as connection:
            with connection.cursor() as cursor:
                query = f"SELECT partattributename FROM uut.dbo.custompartattributes WHERE parttermid = {part_id}"
                results = cursor.execute(query).fetchall()

        columns = default_columns + [row.partattributename for row in results]


        if not blank_template:
            csv_stream = create_templates_df_cpa_prefilled_sku(default_columns, columns, part_id, cpa_type, cpa_category)
        else:
            csv_stream = create_templates_df_cpa(columns, part_id)

        # Send CSV file as an attachment
        return send_file(
            csv_stream,
            as_attachment=True,
            download_name=f"cpa_{timestamp}.xlsx",  # file name is taken care in spinner.js
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        logger.exception(f'An error occurred: {str(e)}')
        response = make_response("An error occurred", 400)
        return response


@app.route('/get_type_values', methods=['POST'])
def get_type_values():
    selected_id = request.json.get('selected_id')

    part_id = selected_id.split('-')[0]

    with db_connector() as connection:
        with connection.cursor() as cursor:
            query = f"select distinct(partattributetype) from uut.dbo.custompartattributes where parttermid = {part_id}"
            results = cursor.execute(query).fetchall()
            values = {
                "types": [types.partattributetype for types in results]
            }
    return jsonify(values)


@app.route('/get_category_values', methods=['POST'])
def get_category_values():

    partattributetype = request.json.get('selected_id')

    TermID = request.json.get('PartId').split('-')[0]

    with db_connector() as connection:
        with connection.cursor() as cursor:
            query = (f"select distinct(partattributecategory) from uut.dbo.custompartattributes where"
                     f" parttermid = {TermID} and partattributetype = '{partattributetype}'")
            results = cursor.execute(query).fetchall()

    values = {
        "categories": [cat.partattributecategory for cat in results]
    }

    return jsonify(values)


if __name__ == '__main__':
    app.run(debug=True)
