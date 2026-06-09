'''Module provides ETL loading of Redshift Target Tables (with optional CDC)'''

from json import loads
from time import time

from boto3 import client, session

from pyspark.sql import Window
from pyspark.sql.functions import col, desc, lit, row_number, when

from glue_utils import get_logger, Logger, MetricsUtil
from glue_clss import AWSGLueJob, KEYS_FOREIGN, KEYS_PRIMARY, PRPS_PARTITION, PRPS_REDSHIFT_ONLY

log: Logger = None

SQL_INJECTION_FORBIDDEN_CHARS = [
    "'", '"', '%', '|', '&', ';', ':', '*', '(', ')', '{', '}', '-', '`',
    chr(0), '\b', '\n', '\r', '\t', chr(26), '\\', ' _'
]


class AWSGLueRedshiftJob(AWSGLueJob):
    '''AWSGLueRedshiftJob is an AWSGLueJob specialized for loading datalake data into Redshift

    :param argv: list of command line parameters (usually sys.argv)
    :type argv: list
    :param req: list of required command line parameters
    :type req: list
    :param optnl: list of optional command line parameters
    :type optnl: list, optional
    '''

    aws_region: str
    auth_params: tuple

    #pylint: disable=redefined-outer-name # using 'argv' b/c that is the essence of it
    #pylint: disable=dangerous-default-value # the empty list seems like a fine default
    def __init__(self, argv: list, req: list, optnl: list = []):
        'Constructor method'

        super().__init__(argv, req, optnl)
        global log #pylint: disable=global-statement # log is a global variable
        log = log or get_logger(self.args['JOB_NAME'], self.args['JOB_NAME'])

        self.aws_region   = None
        self.auth_params  = None

        # The table's metadata must exist and at least contain 'preactions' and 'postactions' to load into Redshift
        assert self.tbl_metadata['preactions']
        assert self.tbl_metadata['postactions']

        log.debug('AWSGLueRedshiftJob constructed :)')


    def __del__(self):
        'AWSGLueRedshiftJob destructor: delete owned objects and destruct the class'
        del self._glue_client
        del self._spark_client
        del self._glue_api
        log.debug('AWSGLueRedshiftJob destructed :)')


    def get_connection_options(self) -> tuple:
        '''Used to obtain the required connection credentials required to establish a JDBC connection to Redshift:
            * username: user parameter to pass to the credential authentication for the redshift datasink write
            * password: password parameter to pass to the credential authentication for the redshift datasink write
            * jdbc_url: url parameter to pass to the credential authentication for the redshift datasink write

        :return: tuple of username, password, and jdbc_url
        :rtype: tuple
        '''

        secrets_manager_client = client('secretsmanager', region_name = self.aws_region)
        response = secrets_manager_client.get_secret_value(SecretId = self.args['redshift_secret'])
        secrets = loads(response['SecretString'])

        return (
            secrets['username'],
            secrets['password'],
            f"jdbc:redshift://{secrets['host']}:{secrets['port']}/{self.args['target_database']}"
        )


    def build_datatype_expr(self) -> list:
        '''Given a schema, this function returns a list of redshift columns cast to the target datatype

        :return: redshift fields as a list of col().cast()s to be in the right order and datatype for loading
        :rtype: list
        '''

        schema = self.get_schema()

        rs_fields = [
            # If this is a Redshift only column, then it doesn't exist in the datalake, call its 'path' metadata for the value:
            fld.metadata['path'](self).cast(fld.metadata['type']).alias(fld.name) \
                if callable(fld.metadata['path']) and PRPS_REDSHIFT_ONLY == fld.metadata.get('purpose')

            # There is no CharType() or VarcharType() in spark, so cast as string
            else col(fld.name).cast('string') \
                if (fld.metadata['type'].startswith('char') or fld.metadata['type'].startswith('varchar')) \

            # All others get cast to their database type:
            else col(fld.name).cast(fld.metadata['type'])

            for fld in schema.fields
            if fld.metadata.get('purpose') in (None, PRPS_REDSHIFT_ONLY)
        ]
        log.trace('rs_fields: %s', rs_fields)
        return rs_fields


    def sanitize_sql(self, sql_expr: str) -> str:
        '''Assert that the sql expression does not contain sql inject characters

        :param sql_expr: expression that is a SQL statement or will be joined into an SQL statement
        :type self: str
        :return: sql_expr or ValueError exception if the expression violates assertions
        :rtype: str
        '''

        for ch in SQL_INJECTION_FORBIDDEN_CHARS:
            assert ch not in sql_expr
        return sql_expr


    def get_eff_flds(self) -> tuple:
        '''Get effectivity field names as these differ per dataset

        :return: tuple effectivity field names (eff_to_fld_nme, eff_fm_fld_nme)
        :rtype: tuple
        '''

        def set_high_value(fld: 'AtomicType') -> 'AtomicType':
            '''Set "high_value" (aka. the infinity date/timestamp value for CDC calculations)
            in metadata for CDC calculations (if not already set) per TIMESTAMP or DATE type

            :param fld: an effective field (from or to) of either DATE or TIMESTAMP type
            :type fld: class: `AtomicType`
            :return: the same as the `fld` parameter with 'high_value' added to field's metadata
            :rtype: AtomicType
            '''

            if fld is not None and not fld.metadata.get('high_value'):
                #pylint: disable=line-too-long # create a DATE or TIMESTAMP for effective "infinity" value
                fld.metadata['high_value'] = f"{fld.metadata.get('type')} '2099-12-31{' 00:00:00.000000+00:00' if 'TIMESTAMP' == fld.metadata.get('type') else ''}'"
            return fld

        tos = [fld for fld in self.get_schema().fields if fld.name.startswith('eff_to_')]
        eff_to_fld_nme = set_high_value(tos[0]) if tos else None
        fms = [fld for fld in self.get_schema().fields if fld.name.startswith('eff_fm_')]
        eff_fm_fld_nme = set_high_value(fms[0]) if fms else None

        return (eff_to_fld_nme, eff_fm_fld_nme)


    def get_cdc_rs_query(self) -> dict:
        '''Generate redshift queries using the table schema (SQL is table agnostic and reusable)

        :return: a dictionary of SQL commands generated using the table schema
        :rtype: dict
        '''

        # Every part of our SQL that isn't hardcoded here must be sanitized to guard against injection attacks.
        keys = [self.sanitize_sql(pk) for pk in self.get_keys({KEYS_FOREIGN, KEYS_PRIMARY})]
        tsc  = self.sanitize_sql(self.args['target_schema'])
        ssc  = self.sanitize_sql(self.args['stage_schema'])
        tbl  = self.sanitize_sql(self.args['target_table_name'])
        exclude_cols = ['original_job_id', 'latest_job_id']
        rdshft_cols = [f.name for f in self.get_schema().fields if f.metadata.get('purpose') == PRPS_REDSHIFT_ONLY]

        eff_to_fld_nme, eff_fm_fld_nme = self.get_eff_flds()
        log.trace('eff flds: %s, %s', eff_to_fld_nme, eff_fm_fld_nme)

        def create_join_on_all(self) -> str:
            return ' and '.join([
                f"({tsc}.{tbl}.{fldnm} = {ssc}.{tbl}.{fldnm}" + \
                  (f" OR {tsc}.{tbl}.{fldnm} is null and {ssc}.{tbl}.{fldnm} is null " if fld.nullable else "") +
                ")"
                for fld in self.get_schema().fields
                if fld.name not in exclude_cols
                if fld.metadata.get('purpose') not in (PRPS_PARTITION, PRPS_REDSHIFT_ONLY)
                if (fldnm := self.sanitize_sql(fld.name))
            ])

        def create_select_cols(self, excl_cols_list) -> str:
            return ', '.join([
                fldnm
                for fld in self.get_schema().fields
                if fld.name not in excl_cols_list
                if fld.metadata.get('purpose') != PRPS_PARTITION
                if (fldnm := self.sanitize_sql(fld.name))

            ])

        sql_keys = ', '.join(keys)
        sql_join_on_keys = ' and '.join([
                (f"(ld.{fldnm} = stg.{fldnm} OR ld.{fldnm} is null and stg.{fldnm} is null)"
                if fld.nullable
                else f"ld.{fldnm} = stg.{fldnm}")
                for fld in self.get_schema().fields
                if fld.metadata.get('key') in (KEYS_PRIMARY, KEYS_FOREIGN)
                if (fldnm := self.sanitize_sql(fld.name))
            ])


        sqls = {
            'target_schema':     tsc,
            'stage_schema':      ssc,
            'target_table_name': tbl
        }

        # Clean out the staging table before writing to it
        sqls['sql_delete_preaction'] = f"delete from {ssc}.{tbl}"

        # Delete duplicates loaded to staging table
        sqls['sql_distinct_stage'] = f"""
create temp table {tbl}_uniquerecs as
select {create_select_cols(self, [])}
from (
    select *, row_number() over (partition by {create_select_cols(self, exclude_cols + rdshft_cols)} order by latest_job_id) as rn
    from {ssc}.{tbl}
) as rnk
where rn = 1;

delete from {ssc}.{tbl};

insert into {ssc}.{tbl} select * from {tbl}_uniquerecs;

drop table {tbl}_uniquerecs
"""

        # Update staging eff_to column (CDC requested tables only)
        sqls['sql_update_staging_eff_to'] = (f"""
with cte as (
    select {sql_keys},{eff_fm_fld_nme.name},coalesce(lead({eff_fm_fld_nme.name}) over (partition by {sql_keys} order by {eff_fm_fld_nme.name}), {eff_to_fld_nme.name}) as lead_eff_fm from {ssc}.{tbl}
)
update {ssc}.{tbl}
set {eff_to_fld_nme.name} = cte.lead_eff_fm
from cte
where {sql_join_on_keys.replace('ld.', 'cte.').replace('stg.', f"{ssc}.{tbl}.")}
and cte.{eff_fm_fld_nme.name} = {ssc}.{tbl}.{eff_fm_fld_nme.name};
""") if self.tbl_metadata.get('CDC') else ''

        # Update staging min_eff_fm_flag (CDC requested tables only) and original_job_id for keys that already exist in the target
        sqls['sql_update_staging'] = f"""
update {ssc}.{tbl} as stg
set original_job_id = ld.original_job_id {', min_eff_fm_flag = 0' if self.tbl_metadata.get('CDC') and 'min_eff_fm_flag' in self.get_schema().fieldNames() else ''}
from {tsc}.{tbl} ld
where {sql_join_on_keys}
{f"and ld.{eff_to_fld_nme.name} = {eff_to_fld_nme.metadata.get('high_value')}" if eff_to_fld_nme else ''}
"""

        # Eliminate outdated records (CDC requested tables only), prevent an older version from being loaded if a newer version already exists
        sqls['sql_delete_outdated_from_staging'] = (f"""
delete from {ssc}.{tbl}
using {tsc}.{tbl}
where {sql_join_on_keys.replace('ld.', f"{tsc}.{tbl}.").replace('stg.', f"{ssc}.{tbl}.")}
and {tsc}.{tbl}.{eff_fm_fld_nme.name}  > {ssc}.{tbl}.{eff_fm_fld_nme.name}
""") if self.tbl_metadata.get('CDC') else ''

        # Delete the records from stage that did not change
        sqls['sql_delete_unchanged_from_staging'] = f"""
delete from {ssc}.{tbl}
using {tsc}.{tbl}
where {create_join_on_all(self)}
"""

        # Expire the old record (CDC requested tables only) in target
        sqls['sql_update_expire_old_version'] = (f"""
update {tsc}.{tbl} as ld
set {eff_to_fld_nme.name} = stg.{eff_fm_fld_nme.name}{', max_eff_to_flag = 0' if 'max_eff_to_flag' in self.get_schema().fieldNames() else ''}
from (
    select {sql_keys},min({eff_fm_fld_nme.name}) as {eff_fm_fld_nme.name}
    from {ssc}.{tbl}
    group by {sql_keys}
) stg
where {sql_join_on_keys}
and ld.{eff_to_fld_nme.name} = {eff_to_fld_nme.metadata.get('high_value')}
""") if self.tbl_metadata.get('CDC') else ''

        # Insert new rows and new versions of existing records
        sqls['sql_insert_to_target'] = f"""
insert into {tsc}.{tbl}
select *
from {ssc}.{tbl}
"""

        # Add any custom sqls if present from the metadata
        if 'custom_sqls' in self.tbl_metadata:
            for key, value in self.tbl_metadata['custom_sqls'].items():
                sqls[key] = value if not callable(value) else value(
                    self,
                    src_schema = ssc,
                    tgt_schema = tsc,
                    tbl_nm = tbl,
                    select_keys = sql_keys,
                    join_keys = sql_join_on_keys,
                    eff_fm = eff_fm_fld_nme.name,
                    sql_delete_unchanged_from_staging = sqls['sql_delete_unchanged_from_staging']
                )

        log.trace("Redshift queries: %s", sqls)

        return {k: v.replace('\n', ' ').strip() for (k, v) in sqls.items()}


    # ETL methods:
    def connect(self, etl_state: dict) -> dict:
        '''Connect to datalake, ensure assets exist

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state (update self.aws_region and self.auth_params)
        :rtype: dict
        '''

        log.info('AWSGLueRedshiftJob connecting to resources:')
        etl_state = super().connect(etl_state)

        # Get AWS Region
        self.aws_region = session.Session().region_name
        log.debug("aws_region: %s", self.aws_region)

        self.auth_params = self.get_connection_options()
        log.trace("username: %s", self.auth_params[0])
        log.trace("password: %s", '*' * len(self.auth_params[1]))
        log.trace("jdbc_url: %s", self.auth_params[2])

        return etl_state


    def transform(self, etl_state: dict) -> dict:
        '''Transform datalake table to a dataframe ready for redshift insert/update. Casts datatypes to target datatypes,
        and adds 'fit_lkdwn_flag', 'original_job_id', and 'latest_job_id' columns.

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''

        if etl_state['dataframe'].isEmpty():
            log.warning('No data or no new data is available in the source table')
            return etl_state

        # Create target dataframe and add JobIds to manage historical data
        df = etl_state['dataframe'].select(
            # Select Redshift columns cast to target datatypes
            self.build_datatype_expr(),
        )

        if self.tbl_metadata.get('CDC'):
            pks = self.get_keys({KEYS_PRIMARY})
            eff_to_fld_nme, eff_fm_fld_nme = self.get_eff_flds()

            window_spec = Window.partitionBy(pks).orderBy(
                col(eff_fm_fld_nme.name),
                lit(1) if eff_to_fld_nme.metadata.get('purpose') == PRPS_REDSHIFT_ONLY else eff_to_fld_nme.name,
                col('latest_job_id')
            )
            window_spec_desc = Window.partitionBy(pks).orderBy(
                desc(col(eff_fm_fld_nme.name)),
                desc(lit(1) if eff_to_fld_nme.metadata.get('purpose') == PRPS_REDSHIFT_ONLY else eff_to_fld_nme.name),
                col('latest_job_id')
            )
            if eff_to_fld_nme.metadata.get('purpose') == PRPS_REDSHIFT_ONLY:
                df = df.withColumn(
                    eff_to_fld_nme.name,
                    lit('2099-12-31T00:00:00.000000Z') if eff_to_fld_nme.metadata.get('type') == 'timestamp' else lit('2099-12-31')
                    .cast(eff_to_fld_nme.dataType)
                )

            if 'max_eff_to_flag' in self.get_schema().fieldNames():
                df = df.withColumn('max_eff_to_flag', when(row_number().over(window_spec_desc) == 1, 1).otherwise(0).cast('SMALLINT'))

            if 'min_eff_fm_flag' in self.get_schema().fieldNames():
                df = df.withColumn('min_eff_fm_flag', when(row_number().over(window_spec) == 1, 1).otherwise(0).cast('SMALLINT'))

        if self.args.get('profile'):
            print('Transformed records:')
            df.printSchema()
            df.show(self.trace_show_cnt, truncate = self.trace_show_truncate, vertical = self.trace_show_vertical)

        etl_state['dataframe'] = df
        return etl_state


    def load(self, etl_state: dict) -> dict:
        '''Write data to redshift stage table and UPSERT/INSERT into target table

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''

        df = etl_state['dataframe']
        if df.isEmpty():
            log.warning('THERE ARE NO ACCEPTED DATA!')
            return etl_state

        # Get the SQL's we will need to CDC our data in Redshift
        sqls = self.get_cdc_rs_query()

        # The table's metadata must contain 'preactions' and 'postactions' to load into Redshift
        preactions_sql  = ';'.join(['BEGIN'] + list(map(lambda sql_key: sqls[sql_key], self.tbl_metadata['preactions']))  + ['END;'])
        postactions_sql = ';'.join(['BEGIN'] + list(map(lambda sql_key: sqls[sql_key], self.tbl_metadata['postactions'])) + ['END;'])

        (username, password, jdbc_url) = self.auth_params

        if self.args.get('profile'):
            print('Load to redshift spark plan:')
            etl_state['dataframe'].explain(mode = self.explain_mode)

        try:
            load_start = time()

            if self.args.get('profile'):
                print('Pre RS write schema and record sample:')
                df.printSchema()
                df.show(self.trace_show_cnt, truncate = self.trace_show_truncate, vertical = self.trace_show_vertical)

            df.write.format(
                'io.github.spark_redshift_community.spark.redshift'
            ).options(
                aws_iam_role        = self.args['redshift_iam_role'],
                user                = username,
                password            = password,
                dbtable             = f"{sqls['stage_schema']}.{sqls['target_table_name']}",
                extracopyoptions    = "TRUNCATECOLUMNS TIMEFORMAT AS 'auto' DATEFORMAT as 'auto'",
                preactions          = preactions_sql,
                postactions         = postactions_sql,
                include_column_list = 'true',
                tempdir             = self.args['redshift_temp_dir'],
                tempformat          = 'CSV',
                url                 = jdbc_url
            ).mode(
                'append'
            ).save()
            load_end = time()

            log.info("Write to Redshift destination table %s completed successfully", sqls['target_table_name'])

            self.metrics.add('TimeToWrite', load_end - load_start, MetricsUtil.SECONDS)

        except Exception:
            log.exception("write error in AWSGLueRedshiftJob.load")
            raise

        etl_state['dataframe'] = df
        return etl_state
