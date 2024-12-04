import io
import os
import time
import uuid
from urllib.parse import urlencode, quote_plus
import flask_login
import pandas as pd
import logging
from datetime import datetime

import xlsxwriter
from authlib.integrations.flask_client import OAuth
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from flask import Flask, request, render_template, redirect, url_for, flash, send_file, jsonify, make_response, session
from flask_login import login_required, LoginManager
from database_connector import db_connector
from upload_controllers import cpa_uploader, part_term_uploader, gbb_uploader
from utils import read_csv_file, create_templates_df_csv_buffer, validate_good_better_best, get_csv_buffer, \
    create_templates_df_cpa, create_templates_df_cpa_prefilled_sku, validate_custom_part_attributes, \
    validate_load_sku_list, write_large_dataset_to_excel

load_dotenv()

environment = 'prd'
app = Flask(__name__)
app.secret_key = os.getenv('APP_SECRET_KEY')

# login_manager = LoginManager()
# login_manager.init_app(app)

GBB_NOTEBOOK_JOB = '528538988059648'
CPA_NOTEBOOK_JOB = '55857284532359'
PT_NOTEBOOK_JOB = '440371801732973'


# logging.basicConfig(
#     filename='app.log',
#     level=logging.INFO,
#     format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
# )

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


class User(flask_login.UserMixin):
    def __init__(self, user_id=None):
        self.id = user_id


w = WorkspaceClient(
  auth_type='pat',
  host=os.getenv('DATABRICKS_HOST'),
  token=os.getenv('DATABRICKS_ACCESS_TOKEN')
)


volume_catalog = 'uut'
volume_schema = 'dbo'
volume_name = 'vol_fileuploadutility'

# oauth = OAuth(app)
#
# oauth.register(
#     "auth0",
#     client_id=os.environ['CLIENT_ID'],
#     client_secret=os.environ['CLIENT_SECRET'],
#     client_kwargs={
#         "scope": "openid profile email",
#     },
#     server_metadata_url=f"https://{os.environ['ORG_URL']}/.well-known/openid-configuration"
# )
#
#
@app.route('/')
def homepage():
    return render_template('homepage.html')
#
#
# @login_manager.user_loader
# def load_user(user_id):
#     return User(user_id)
#
#
# @app.route("/login")
# def login():
#     return oauth.auth0.authorize_redirect(
#         redirect_uri=f"{os.environ['CALLBACK_URL']}/callback"
#     )
#
#
# @app.route("/callback", methods=["GET", "POST"])
# def callback():
#     token = oauth.auth0.authorize_access_token()
#     session["user"] = token
#     user_info = token.get('userinfo')
#     user_id = user_info.get('sub') if user_info else None
#     if not user_id:
#         return "Error: User ID not found", 400
#     user = User(user_id)
#     flask_login.login_user(user)
#     return redirect(url_for('homepage'))
#
#
# @app.route("/logout")
# # @login_required
# def logout():
#     session.clear()
#     flask_login.logout_user()
#     return redirect(
#         "https://" + os.environ['ORG_URL']
#         + "/v2/logout?"
#         + urlencode(
#             {
#                 "returnTo": os.environ['CALLBACK_URL'],
#                 "client_id": os.environ['CLIENT_ID'],
#             },
#             quote_via=quote_plus,
#         )
#     )


@app.route('/upload_form')
# @login_required
def upload_form():
    return render_template('upload.html')


@app.route('/upload_form_part_term')
# @login_required
def upload_form_part_term():
    return render_template('upload_part_term.html')


@app.route('/upload_load_sku_list')
# @login_required
def upload_load_sku_list():
    return render_template('loadskulist_template.html')

@app.route('/load_sku_options')
# @login_required
def load_sku_options():
    upload_type = request.args.get('upload_type')

    if upload_type == 'linecode_partnumber':
        return render_template('loadskulist_lc_part.html', upload_type=upload_type)
    elif upload_type == 'sku':
        return render_template('loadskulist_lc_part.html', upload_type=upload_type)
    elif upload_type == 'partnumber_only':
        return render_template('loadskulist_lc_part.html', upload_type=upload_type)
    elif upload_type == 'partnumber_only_with_filters':
        checkbox_options = ["9B", "9S", "9T", "A1", "A8", "AC", "AE", "AK", "AQ", "AU", "AZ", "B0", "BA",
                            "BD", "BF","9B", "9S", "9T", "A1", "A8", "AC", "AE", "AK", "AQ", "AU", "AZ",
                            "B0", "BA", "BD", "BF","9B", "9S", "9T", "A1", "A8", "AC", "AE", "AK", "AQ",
                            "AU", "AZ", "B0", "BA", "BD", "BF"]
        return render_template('loadskulist_lc_part.html', upload_type=upload_type,  options=checkbox_options)


@app.route('/upload_form_cust_part_attr')
# @login_required
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
        print(f'An error occurred: {str(e)}')
        logger.exception(f'An error occurred: {str(e)}')
        return redirect(url_for('homepage'))


@app.route('/upload_csv', methods=['POST'])
# @login_required
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
            random_uuid = str(uuid.uuid4().int)[:5]
            filename = f'{file.filename[:-5]}_{random_uuid}.csv'
            df = pd.read_excel(file)
            df = validate_good_better_best(df, required_columns, nullable_columns)
            csv_buffer = get_csv_buffer(df)

            file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_folder}/{filename}"
            w.files.upload(file_path=file_path_in_volume, contents=csv_buffer, overwrite=True)

            gbb_uploader(file, file_path_in_volume)

            # notebook_params = dict(
            #     file_name=filename
            #     # ,uploader_email=session.get('user').get('userinfo')['email']
            # )
            # run_by_id = w.jobs.run_now(job_id=GBB_NOTEBOOK_JOB, notebook_params=notebook_params).result()
            # run_results = w.jobs.get_run_output(run_id=run_by_id.tasks[0].run_id).notebook_output.result
            #
            # if run_results and 'Error' in run_results:
            #     raise Exception(run_results)

            flash('File uploaded and data inserted successfully!', category='success')
            return redirect(url_for('upload_form'))

        except Exception as e:
            flash(f'An error occurred: Please try again', category='error')
            print(f'An error occurred: {str(e)}')
            logger.exception(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form'))

    else:
        flash('Invalid file format. Please upload a XLSX.', category='error')
        return redirect(url_for('upload_form'))


@app.route('/upload_part_term', methods=['POST'])
# @login_required
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
            random_uuid = str(uuid.uuid4().int)[:5]
            filename = f'{file.filename[:-5]}_{random_uuid}.csv'
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
            part_term_uploader(file, file_path_in_volume)

            # notebook_params = dict(
            #     file_name=filename
            #     # ,uploader_email=session.get('user').get('userinfo')['email']
            # )
            # run_by_id = w.jobs.run_now(job_id=PT_NOTEBOOK_JOB, notebook_params=notebook_params).result()
            # run_results = w.jobs.get_run_output(run_id=run_by_id.tasks[0].run_id).notebook_output.result
            #
            # if run_results and 'Error' in run_results:
            #     raise Exception(run_results)

            flash('File uploaded and data inserted successfully!', category='success')

            # cleanup
            # w.jobs.delete(job_id=jobid.job_id)
            return redirect(url_for('upload_form_part_term'))

        except Exception as e:
            flash(f'An error occurred: Please try again', category='error')
            print(f'An error occurred: {str(e)}')
            logger.exception(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form_part_term'))

    else:
        flash('Invalid file format. Please upload a XLSX.', category='error')
        return redirect(url_for('upload_form_part_term'))


@app.route('/upload_custom_part_attributes', methods=['POST'])
# @login_required
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
            random_uuid = str(uuid.uuid4().int)[:5]
            filename = f'{file.filename[:-5]}_{random_uuid}.csv'
            df = pd.read_excel(file)
            df = validate_custom_part_attributes(df, required_columns, nullable_columns)
            csv_buffer = get_csv_buffer(df)
            file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_folder}/{filename}"
            w.files.upload(file_path=file_path_in_volume, contents=csv_buffer, overwrite=True)

            cpa_uploader(file, file_path_in_volume)

            # notebook_params = dict(
            #     file_name=filename
            #     # , uploader_email=session.get('user').get('userinfo')['email']
            # )
            #
            # run_by_id = w.jobs.run_now(job_id=CPA_NOTEBOOK_JOB, notebook_params=notebook_params).result()
            #
            # run_results = w.jobs.get_run_output(run_id=run_by_id.tasks[0].run_id).notebook_output.result
            #
            # if run_results and 'Error' in run_results:
            #     raise Exception(run_results)

            flash('File uploaded and data inserted successfully!', category='success')
            return redirect(url_for('upload_form_cust_part_attr'))

        except Exception as e:
            flash(f'An error occurred: Please try again', category='error')
            print(f'An error occurred: {str(e)}')
            logger.exception(f'An error occurred: {str(e)}')
            return redirect(url_for('upload_form_cust_part_attr'))

    else:
        flash('Invalid file format. Please upload a XLSX.', category='error')
        return redirect(url_for('upload_form_cust_part_attr'))


@app.route('/upload_load_sku_list_for_interchanges', methods=['POST'])
# @login_required
def upload_load_sku_list_for_interchanges():

    line_code_column = request.form.get('linecode_column')
    part_number_column = request.form.get('partnumber_column')
    sku_column = request.form.get('sku_column')
    load_sku = request.form.get('load_sku')
    only_part_number = False
    required_columns = ['linecode', 'partnumber']

    if request.form.get('filter_by'):
        filter_column = request.form.get('filter_by')
        filter_data = request.form.getlist('lsku-filter-by-data')


    if load_sku and load_sku == 'true':
        only_part_number = True
        required_columns = ['partnumber']

    initial_column = []

    if line_code_column and part_number_column:
        initial_column = ['`linecode`', '`partnumber`']

    elif sku_column:
        initial_column = ['`sku`']
    elif only_part_number:
        initial_column = ['`partnumber`']


    timestamp = datetime.now()
    if 'file' not in request.files:
        flash('No file part', category='error')
        return redirect(request.url)

    file = request.files['file']

    if file.filename == '':
        flash('No selected file', category='error')
        return redirect(request.url)

    if file and file.filename.endswith('.xlsx'):
        try:
            df = pd.read_excel(file)
            if line_code_column:
                if 'linecode' in df.columns and line_code_column!='linecode':
                    df.rename(columns={'linecode': 'orig_linecode'}, inplace=True)
                df.rename(columns={line_code_column: 'linecode'}, inplace=True)
            if part_number_column:
                if 'partnumber' in df.columns and part_number_column != 'partnumber':
                    df.rename(columns={'partnumber': 'orig_partnumber'}, inplace=True)
                df.rename(columns={part_number_column: 'partnumber'}, inplace=True)
            if sku_column:
                if 'sku' in df.columns and part_number_column and part_number_column != 'sku':
                    df.rename(columns={'sku': 'orig_sku'}, inplace=True)
                if not line_code_column and not part_number_column:
                    required_columns = ['sku']
                df.rename(columns={sku_column: 'sku'}, inplace=True)
            df = df.fillna('')
            validate_load_sku_list(df, required_columns, only_part_number)

            if not only_part_number:

                if 'sku' in df.columns and sku_column:
                    inline_table_alias = 'b'
                    values_clause = ", ".join(
                        [f"({', '.join(map(repr, row))})" for row in df.values]
                    )
                    column_clause_list = [f"`{col}`" for col in df.columns]
                    columns_clause = ", ".join(column_clause_list)
                    columns_clause_2_list = [col for col in column_clause_list if (col not in initial_column) and (col != '`sku`')]
                    columns_clause_2 = ", ".join(columns_clause_2_list)
                    columns_clause_2 += ',' if columns_clause_2_list else ''
                    initial_column_clause = ", ".join(initial_column)
                    initial_column_clause += ',' if initial_column else ''
                    inner_column_clause_list = [f"`b`.`{col}`" for col in df.columns]
                    inner_column_clause = ", ".join(inner_column_clause_list)
                    inner_column_clause += ',' if inner_column_clause_list else ''
                else:
                    inline_table_alias = 'b'
                    df['sku'] = (df['linecode'] + df['partnumber']).replace(r'[^a-zA-Z0-9]', '', regex=True)
                    values_clause = ", ".join(
                        [f"({', '.join(map(repr, row))})" for row in df.values]
                    )
                    column_clause_list = [f"`{col}`" for col in df.columns]
                    columns_clause = ", ".join(column_clause_list)
                    columns_clause_2_list = [col for col in column_clause_list if (col not in initial_column) and (col != '`sku`')]
                    columns_clause_2 = ", ".join(columns_clause_2_list)
                    columns_clause_2 += ',' if columns_clause_2_list else ''
                    initial_column_clause = ", ".join(initial_column)
                    initial_column_clause += ',' if initial_column else ''
                    inner_column_clause_list = [f"`b`.`{col}`" for col in df.columns]
                    inner_column_clause = ", ".join(inner_column_clause_list)
                    inner_column_clause += ',' if inner_column_clause_list else ''

                with db_connector() as connection:
                    with connection.cursor() as cursor:
                        fetch_query = f"""
                        select query from uut.dbo.apps_query_table
                        where app_name = 'load_sku_for_interchanges' and name='sku_only'
                        """
                        rows = cursor.execute(fetch_query).fetchone()[0]

                        query = rows.format(
                            columns_clause=columns_clause,
                            values_clause=values_clause,
                            initial_column_clause=initial_column_clause,
                            columns_clause_2=columns_clause_2,
                            inner_column_clause=inner_column_clause,
                            inline_table_alias=inline_table_alias
                        )


#                 query = f"""
#                       WITH inline_table ({columns_clause}) AS (
#                             VALUES{values_clause})
#                       SELECT
#                             {initial_column_clause}
#                             a_sku as sku,
#                             a_interchangesku as interchangesku,
#                             iss_InterchDeadNet as InterchDeadNet,
#                             gbb_description as gbbdescription,
#                             {columns_clause_2}
#                             iss_partterminologykey as partterminologykey,
#                             family_gskuid as familygskuid,
#                             gsku_id as gskuid,
#                             oh_qty as  SupStk,
#                             Sup_Hist as SupHist,
#                             Internet_Qty_Sold as InternetQtySold,
#                             Bulls_Eye as BullsEye,
#                             PBE_Qty_Sold as PBEQtySold,
#                             LineCode_key as LineCodekey,
#                             Product_GroupKey as ProductGroupKey,
#                             Car_line as Carline,
#                             oh_ext_abc as OnHandExtAbc,
#                             SupHist_Sales_Extended as SupHistSalesExtended,
#                             Internet_Sales_Extended as InternetSalesExtended,
#                             BESales_Extended as BESalesExtended,
#                             PBE_Sales_Extended as PBESalesExtended,
#                             a_groupnumber as groupnumber
#                         FROM (
#                             SELECT
#                                 {inner_column_clause}
#                                 iss.partterminologykey as iss_partterminologykey,
#                                 a.groupnumber as a_groupnumber,
#                                 a.sku as a_sku,
#                                 a.interchangesku as a_interchangesku,
#                                 a.gbbdescription as gbb_description,
#                                 interchangesource as inter_change_source,
# --                                 groupnumber as group_number,
#                                 intergroupnumber as inter_group_number,
#                                 groupid AS gsku_id,
#                                 a.familygskuid as family_gskuid,
#                                 iss.unitcostdeadnet AS iss_InterchDeadNet,
#                                 demis.SupHist as Sup_Hist,
#                                  demis.InternetQtySold as Internet_Qty_Sold,
#                                  demis.BullsEye as Bulls_Eye,
#                                  demis.PBEQtySold as PBE_Qty_Sold,
#                                  iss.LineCodekey as LineCode_key,
#                                  iss.ProductGroupKey as Product_GroupKey,
#                                  iss.Carline as Car_line,
#                                demis.SupHistSalesExtended as SupHist_Sales_Extended,
#                                demis.InternetSalesExtended as Internet_Sales_Extended,
#                                demis.BESalesExtended as BESales_Extended,
#                                demis.PBESalesExtended as PBE_Sales_Extended,
#                                ohqty as oh_qty,
#                                ohextabc as oh_ext_abc
#                             FROM catalogdata.gold.interchangelookupcurated a
#                             JOIN inline_table  {inline_table_alias} ON a.sku = {inline_table_alias}.sku
#                             JOIN dst.gold.skumaster iss ON a.interchangesku = iss.sku
#                             JOIN (
#                                 SELECT
#                                     sku,
#                                     suphist,
#                                     suphistsalesextended,
#                                     purebullseyeqtysold AS PBEQtySold,
#                                     purebullseyesalesextended AS PBESalesExtended,
#                                     internetqtysold,
#                                     internetsalesextended,
#                                     (suphist - internetqtysold) AS BullsEye,
#                                     (suphistsalesextended - internetsalesextended) AS BESalesExtended
#                                 FROM dst.gold.vwdemands_pivot
#                                 GROUP BY sku, suphist, suphistsalesextended, purebullseyeqtysold, purebullseyesalesextended, internetqtysold, internetsalesextended
#                             ) demis ON a.interchangesku = demis.sku
#                             join (select a.sku ,sum(onhandquantity) ohqty,sum(onhandextended) ohextabc from dst.gold.invsummary a  where sbran<>888
#                             group by a.sku ) inv on a.interchangesku = inv.sku
#                          ) subquery
#                          order by a_sku asc, Sup_Hist desc
#                          """
            else:
                clean_column = 'partnumber_cleansed'
                inline_table_alias = 'b'
                df[f'{clean_column}'] = df['partnumber'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True)
                values_clause = ", ".join(
                    [f"({', '.join(map(repr, row))})" for row in df.values]
                )

                column_clause_list = [f"`{col}`" for col in df.columns]
                columns_clause = ", ".join(column_clause_list)
                columns_clause_2_list = [col for col in column_clause_list if col not in initial_column]
                columns_clause_2_list.remove(f'`{clean_column}`')
                columns_clause_2 = ", ".join(columns_clause_2_list)
                columns_clause_2 += ',' if columns_clause_2_list else ''
                initial_column_clause = ", ".join(initial_column)
                initial_column_clause += ',' if initial_column else ''
                inner_column_clause_list = [f"`{inline_table_alias}`.`{col}`" for col in df.columns]
                inner_column_clause = ", ".join(inner_column_clause_list)
                inner_column_clause += ',' if inner_column_clause_list else ''


                if not request.form.get('filter_by'):
                    with db_connector() as connection:
                        with connection.cursor() as cursor:
                            fetch_query = f"""
                                select query from uut.dbo.apps_query_table
                                where app_name = 'load_sku_for_interchanges' and name='partnumber_only'
                            """
                            rows = cursor.execute(fetch_query).fetchone()[0]

                            query = rows.format(
                                columns_clause=columns_clause,
                                values_clause=values_clause,
                                initial_column_clause=initial_column_clause,
                                columns_clause_2=columns_clause_2,
                                inner_column_clause=inner_column_clause,
                                inline_table_alias=inline_table_alias,
                                clean_column=clean_column
                            )
                else:
                    filter_clause = "','".join(filter_data)
                    with db_connector() as connection:
                        with connection.cursor() as cursor:
                            fetch_query = f"""
                                select query from uut.dbo.apps_query_table
                                where app_name = 'load_sku_for_interchanges' and name='partnumber_only_with_filter'
                            """
                            rows = cursor.execute(fetch_query).fetchone()[0]

                            query = rows.format(
                                columns_clause=columns_clause,
                                values_clause=values_clause,
                                initial_column_clause=initial_column_clause,
                                columns_clause_2=columns_clause_2,
                                inner_column_clause=inner_column_clause,
                                inline_table_alias=inline_table_alias,
                                clean_column=clean_column,
                                filter_clause=filter_clause,
                                filter_column=filter_column
                            )
#                 query = f"""
#                           WITH inline_table ({columns_clause}) AS (
#                                 VALUES{values_clause})
#                            SELECT {initial_column_clause}
#                                 subquery.a_sku as sku,
#                                 ICSKU as interchangesku,
#                                 IDN as InterchDeadNet,
#                                 GBBD as gbbdescription,
#                                 {columns_clause_2}
#                                 Line_Code as LineCode,
#                                 PTK as partterminologykey,
#                                 PTName as partterminologyname,
#                                 FGSKUID as familygskuid,
#                                 a_gskuid as gskuid,
# --                                 subquery.partnumber,
#                                 oh_qty as SupStk,
#                                 SH as SupHist,
#                                 IQSD as InternetQtySold,
#                                 BUEYE as BullsEye,
#                                 PBEQSD as PBEQtySold,
#                                 ISS_LCK as LineCodekey,
#                                 ISS_PGKY as ProductGroupKey,
#                                 CANE as Carline,
#                                 oh_ext_abc as  OnHandExtAbc,
#                                 SHSEDD as SupHistSalesExtended,
#                                 D_ISEDD as InternetSalesExtended,
#                                 BESEDD as BESalesExtended,
#                                 PBSEDD as PBESalesExtended,
#                                 GNBR as groupnumber
#                             FROM (
#                                 SELECT {inner_column_clause}
#                                     iss.partterminologykey as PTK,
#                                     iss.partterminologyname as PTName,
#                                     iss.linecode as Line_Code,
#                                     a.groupnumber as GNBR,
#                                     a.sku as a_sku,
#                                     a.interchangesku as ICSKU,
#                                     a.gbbdescription as GBBD,
#                                     interchangesource as ICSRC,
#                                     intergroupnumber as IGN,
#                                     groupid AS a_gskuid,
#                                     a.familygskuid as FGSKUID,
#                                     iss.unitcostdeadnet AS IDN,
#                                     demis.SupHist AS SH,
#                                      demis.InternetQtySold AS IQSD,
#                                       demis.BullsEye AS BUEYE,
#                                        demis.PBEQtySold AS PBEQSD,
#                                     iss.LineCodekey as ISS_LCK,
#                                     iss.ProductGroupKey as ISS_PGKY,
#                                     iss.Carline as CANE,
#                                    demis.SupHistSalesExtended as SHSEDD,
#                                     demis.InternetSalesExtended as D_ISEDD,
#                                     demis.BESalesExtended as BESEDD,
#                                      demis.PBESalesExtended as PBSEDD,
#                                      ohqty as oh_qty,
#                                      ohextabc as oh_ext_abc
#                                 FROM catalogdata.gold.interchangelookupcurated a
#                                 JOIN inline_table  {inline_table_alias} ON a.udfpn = {inline_table_alias}.{clean_column}
#                                 JOIN dst.gold.skumaster iss ON a.interchangesku = iss.sku
#                                 JOIN (
#                                     SELECT
#                                         sku,
#                                         suphist,
#                                         suphistsalesextended,
#                                         purebullseyeqtysold AS PBEQtySold,
#                                         purebullseyesalesextended AS PBESalesExtended,
#                                         internetqtysold,
#                                         internetsalesextended,
#                                         (suphist - internetqtysold) AS BullsEye,
#                                         (suphistsalesextended - internetsalesextended) AS BESalesExtended
#                                     FROM dst.gold.vwdemands_pivot
#                                     GROUP BY sku, suphist, suphistsalesextended,
#                                      purebullseyeqtysold, purebullseyesalesextended,
#                                       internetqtysold, internetsalesextended
#                                 ) demis ON a.interchangesku = demis.sku
#                                 join (select a.sku ,sum(onhandquantity)
#                                 ohqty,sum(onhandextended) ohextabc from
#                                 dst.gold.invsummary a  where sbran<>888
#                                 group by a.sku ) inv on a.interchangesku = inv.sku
#                                 where iss.{filter_column} in ('{filter_clause}')
#                              ) subquery
#                              order by subquery.a_sku asc, SH desc
#                         """

            with db_connector() as connection:
                with connection.cursor() as cursor:
                    start_time = time.time()
                    rows = cursor.execute(query).fetchall()
                    if not rows:
                        flash('No Records Found For The Given Data', category='info')
                        return jsonify(
                            {'status': 'error', 'message': 'No Records Found For The Given Data'})
                    columns = [desc[0] for desc in cursor.description]
                    end_time = time.time()
            s_time = time.time()

            data_stream = write_large_dataset_to_excel(columns, rows)

            execution_time = end_time - start_time
            e_time = time.time()
            t_time = e_time - s_time
            print(f"Execution time: {execution_time:.2f} seconds")
            print(f"Execution time: {t_time:.2f} seconds")

            flash('File Downloaded successfully!', category='success')
            return send_file(
                data_stream,
                as_attachment=True,
                download_name=f"load_sku_{timestamp}.xlsx",  # file name is taken care in loadskulist_template.html
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        except Exception as e:
            flash(f'An error occurred: Please try again', category='error')
            logger.exception(f'An error occurred: {str(e)}')
            print(f'An error occurred: {str(e)}')
            return jsonify({'status': 'error', 'message': 'An error occurred while uploading the SKU list.'})

    else:
        flash('Invalid file format. Please upload a XLSX.', category='error')
        return redirect(url_for('load_sku_options'))


@app.route('/download_gbb_template', methods=['GET'])
# @login_required
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
        print(f'An error occurred: {str(e)}')
        return redirect(url_for('upload_form'))


@app.route('/download_cpa_template', methods=['GET'])
# @login_required
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
                query = f"SELECT partattributename FROM uut.dbo.custompartattributes_header WHERE parttermid = {part_id}"
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
        print(f'An error occurred: {str(e)}')
        response = make_response("An error occurred", 400)
        return response


@app.route('/get_type_values', methods=['POST'])
# @login_required
def get_type_values():
    selected_id = request.json.get('selected_id')

    part_id = selected_id.split('-')[0]

    with db_connector() as connection:
        with connection.cursor() as cursor:
            query = f"select distinct(partattributetype) from uut.dbo.custompartattributes_header where parttermid = {part_id}"
            results = cursor.execute(query).fetchall()
            values = {
                "types": [types.partattributetype for types in results]
            }
    return jsonify(values)


@app.route('/get_category_values', methods=['POST'])
# @login_required
def get_category_values():

    partattributetype = request.json.get('selected_id')

    TermID = request.json.get('PartId').split('-')[0]

    with db_connector() as connection:
        with connection.cursor() as cursor:
            query = (f"select distinct(partattributecategory) from uut.dbo.custompartattributes_header where"
                     f" parttermid = {TermID} and partattributetype = '{partattributetype}'")
            results = cursor.execute(query).fetchall()

    values = {
        "categories": [cat.partattributecategory for cat in results]
    }

    return jsonify(values)

@app.route('/fetch_columns', methods=['POST'])
def fetch_columns():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    try:
        # Process the file as an Excel file
        df = pd.read_excel(file, nrows=0)
        columns = df.columns.tolist()  # Extract column names
        return jsonify({'columns': columns})
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/fetch_loadsku_partonly_filters', methods=['POST'])
def fetch_loadsku_partonly_filters():
    file = request.files['file']
    filter_by = request.form.get('filter_by')
    filter_data = []
    df = pd.read_excel(file)
    part_number_column = request.form.get('partnumber_column')
    df[f'partnumbercleansed'] = df[part_number_column].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True)
    columns = df.columns.tolist()
    values_clause = ", ".join(
        [f"({', '.join(map(repr, row))})" for row in df.values]
    )
    column_clause_list = [f"`{col}`" for col in df.columns]
    columns_clause = ", ".join(column_clause_list)

    if filter_by == 'partterminologykey':
        with db_connector() as connection:
            with connection.cursor() as cursor:
                query = f"""
                WITH inline_table ({columns_clause}) AS (
                VALUES{values_clause})
                select distinct partterminologykey from (
                 Select partterminologykey, partnumbercleansed 
                from dst.gold.skumaster a 
                join inline_table b on a.partnumbernospecialcharacters=b.partnumbercleansed 
                group by partterminologykey, partnumbercleansed
                )
                """
                results = cursor.execute(query).fetchall()
                filter_data = [ptk[0] for ptk in results]

    elif filter_by == 'linecode':
        with db_connector() as connection:
            with connection.cursor() as cursor:
                query = f"""
                WITH inline_table ({columns_clause}) AS (
                VALUES{values_clause})
                select distinct linecode from (
                 Select linecode, linecodekey,partnumbercleansed 
                from dst.gold.skumaster a 
                join inline_table b on a.partnumbernospecialcharacters=b.partnumbercleansed 
                group by linecode, linecodekey, partnumbercleansed
                )
                """
                results = cursor.execute(query).fetchall()
                filter_data = [lc[0] for lc in results]
    return jsonify({'filter_by': filter_data})



if __name__ == '__main__':
    app.run(debug=True)

