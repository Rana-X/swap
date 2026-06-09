'''Module provides ETL operations from curation to datalake to redshift'''

from copy import deepcopy
from datetime import date, datetime, timedelta
from importlib import import_module
from json import dumps
from os import path
from time import time
from urllib import parse

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import SelectFromCollection
from awsgluedq.transforms import EvaluateDataQuality
from boto3 import client

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, input_file_name, lit, split, lpad, min as cmin, struct, transform
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from glue_utils import get_logger, Logger, MetricsUtil
from job_clss import AWSJob, GlueClient, SparkClient

log: Logger = None

KEYS_FOREIGN         = 'f'
KEYS_PRIMARY         = 'p'
KEYS_REDSHIFT_COND   = 'r'

PRPS_PARTITION       = 'p'
PRPS_REDSHIFT_ONLY   = 'r'


class AWSGLueJob(AWSJob):
    '''AWSGLueJob is an AWSJob specialized to ETL from curated to datalake

    :param argv: list of command line parameters (usually sys.argv)
    :type argv: list
    :param req: list of required command line parameters
    :type req: list
    :param optnl: list of optional command line parameters
    :type optnl: list, optional

    :raises ValueError: schemas must be a dictionary or a callable that returns a dictionary
    '''

    _glue_api: 'client'
    _glue_client: GlueClient
    _spark_client: SparkClient
    schemas: dict
    tbl_metadata: dict

    explain_mode: str = 'formatted'


    #pylint: disable=redefined-outer-name # using 'argv' b/c that is the essence of it
    #pylint: disable=dangerous-default-value # the empty list seems like a fine default
    def __init__(self, argv: list, req: list, optnl: list = []):
        'Constructor method'

        super().__init__(argv, req, optnl)
        global log #pylint: disable=global-statement # log is a global variable
        log = log or get_logger(self.args['JOB_NAME'], self.args['JOB_NAME'])
        self._glue_api = None
        self._glue_client = None
        self._spark_client = None
        self.trace_show_cnt = 2
        self.trace_show_truncate = False
        self.trace_show_vertical = False

        self.schemas = {}

        # Look thru all args ending in 'schema' and whose value ends with 'schemas'. E.g. --schema=flightplan_schemas; or --parent_schema=fuel_schemas
        for schema_arg in filter(lambda w: w.endswith('schema') and self.args[w].endswith('schemas'), self.args.keys()):
            schema_modname = self.args[schema_arg]
            log.trace("Loading schemas from: %s", schema_modname)

            schemas = (import_module(schema_modname)).schemas
            if callable(schemas):
                schemas = schemas(self)

            if not isinstance(schemas, dict):
                log.error("schemas parameter did not yield a valid schema dictionary: %s", schemas)
                raise ValueError('schemas parameter did not yield a valid schema dictionary')

            for tbl, schema in schemas.items():
                if 'metadata' == tbl:
                    self.tbl_metadata = schema.get(self.args['target_table_name'], {})
                else:
                    # Fill in path for all the fields that didn't need it (aka. normalize the metadata):
                    for fld in schema.fields:
                        if 'path' not in fld.metadata:
                            fld.metadata['path'] = fld.name
                # end: if 'metadata' == tbl

            log.debug('Schemas loaded %s', schemas)
            self.schemas.update(schemas)

        if not self.schemas:
            log.debug('No schemas loaded')

        # end: for schema_arg in ...

        log.debug('AWSGLueJob constructed :)')


    def __del__(self):
        'AWSGLueJob destructor: delete owned objects and destruct the class'
        del self._glue_client
        del self._spark_client
        del self._glue_api
        log.debug('AWSGLueJob destructed :)')


    @property
    def spark(self) -> 'SparkSession':
        '''Creates a SparkSession connection 'on demand' to allow locally running jobs to complete

        :return: SparkSession connected to our spark context
        :rtype: class: `SparkSession`
        '''

        if not self._spark_client:
            self._spark_client = SparkClient.connect(self.conf['spark'])
        return self._spark_client.spark


    @property
    def glue_api(self):
        '''Creates a glue boto3.client 'on demand'

        :return: boto3 client for glue api calls
        :rtype: class: `Glue`
        '''

        if not self._glue_api:
            self._glue_api = client('glue')
            log.debug("Connected to glue_api=%s", type(self._glue_api))
            log.debug("Connected to glue_api=%s", self._glue_api)
        return self._glue_api


    @property
    def glue_client(self) -> GlueClient:
        '''Creates a GlueClient connection 'on demand' to allow locally running jobs to complete

        :return: GlueClient connected to our spark context
        :rtype: class: `GlueClient`
        '''

        if not self._glue_client:
            self._glue_client = GlueClient.connect(self.spark.sparkContext, self.args)
        return self._glue_client


    @property
    def glue_context(self) -> GlueContext:
        '''Gets the glue context, creating a GlueClient connection if not already defined

        :return: GlueContext connected to our spark context
        :rtype: class: `GlueContext`
        '''

        return self.glue_client.glue_context


    @classmethod
    def get_default_push_down_predicate(cls, beg: date = None, end: date = None, history_days: int = 0, future_days: int = 0) -> str:
        '''Calculates a push down predicate when None is supplied

        :param beg: datetime.date(year, month, day) for the predicate (defaults to 10 days ago)
        :type beg: date, optional
        :param end: datetime.date(year, month, day) for the predicate (defaults to 2 days future)
        :type end: date, optional
        :return: push down predicate based on current date
        :rtype: str
        '''

        b = beg or (date.today() - timedelta(days = history_days))
        e = end or (date.today() + timedelta(days = future_days))

        bstr = datetime.strftime(b, "year > %Y or (year == %Y and (month > %m or (month == %m and day >= %d)))")
        estr = datetime.strftime(e, "year < %Y or (year == %Y and (month < %m or (month == %m and day <= %d)))")

        return f"({bstr}) and ({estr})"


    def get_schema(self, tbl: str = None) -> StructType:
        '''Given a table name, return the schema

        :param tbl: table name - defaults to 'target_table_name' job argument
        :type tbl: str, optional
        :raises KeyError: if **tbl** is not a valid table name in a schema
        :return: StructType of the table
        :rtype: class `StructType`
        '''

        glue_tbl = tbl or self.args['target_table_name']

        if self.schemas is None:
            log.error("ERROR no schemas loaded, unknown table: %s", tbl)
            raise KeyError(tbl)
        if glue_tbl not in self.schemas:
            log.error("ERROR unknown table: %s; known tables: %s", tbl, self.schemas.keys())
            raise KeyError(glue_tbl)

        return self.schemas[glue_tbl]


    def get_table_with_type(self, typ: str) -> str:
        '''Return the name of the table with metadata {typ} = True

        :param typ: string type name (e.g. stats or curated)
        :type typ: str
        :return: Name of the table
        :rtype: str
        '''

        stat_nms = [tbl for (tbl, mdata) in self.schemas.get('metadata', {}).items() if mdata.get(typ)]
        return stat_nms and stat_nms[0]


    def get_keys(self, matches: set) -> list[str]:
        '''Scan the schema and return a list of primary keys

        :Example: ``pks = self.get_keys({KEYS_PRIMARY, KEYS_REDSHIFT_COND}) # get both primary key and redshift upsert key``

        :param matches: primary keys can any of KEYS_FOREIGN, KEYS_PRIMARY, and/or KEYS_REDSHIFT_COND
        :type matches: set
        :return: list of primary key field names
        :rtype: list[str]
        '''

        log.trace("Looking for keys that match: %s", matches)

        return [
            fld.name
            for fld in self.get_schema(self.args['target_table_name']).fields
            if fld.metadata.get('key') in matches
        ]


    def get_table(self, tbl: str = None) -> dict:
        '''Check to see if datalake table already exists; return definition if it does.
        Database is assumed to be the 'target_database' job argument.

        :param tbl: table name - defaults to 'target_table_name' job argument
        :type tbl: str, optional
        :return: table definition or None (is truthy)
        :rtype: dict
        '''

        glue_tbl = tbl or self.args['target_table_name']
        log.info("Checking database/table: %s.%s", self.args['target_database'], glue_tbl)
        try:
            return self.glue_api.get_table(DatabaseName = self.args['target_database'], Name = glue_tbl)

        except (self.glue_api.exceptions.AccessDeniedException, self.glue_api.exceptions.EntityNotFoundException):
            log.exception("Database or Table does not exist or permissions are denied: %s.%s", self.args['target_database'], glue_tbl)
        return {}


    def get_partition_keys(self, tbl: str = None, with_types: bool = False) -> list:
        '''Return list of fields from table schema defining the partition.

        :param tbl: table name - defaults to 'target_table_name' job argument
        :type tbl: str, optional
        :param with_types: return a dictionary with Type(s) (useful for glue api calls); or a list of field names
        :type with_types: str, optional
        :return: partition field list (either a list of dict if 'with_types', else a list of strings)
        :rtype: list
        '''

        glue_tbl = tbl or self.args['target_table_name']
        return [
            {'Name': fld.name, 'Type': fld.dataType.simpleString()} if with_types else fld.name
            for fld in self.get_schema(glue_tbl).fields
            if PRPS_PARTITION == fld.metadata.get('purpose')
        ]


    def get_partition(self, partition: list, tbl: str = None) -> dict:
        '''Check to see if datalake table already exists; return definition if it does.
        Database and Table are assumed to be the 'target_database'/'target_table_name' job arguments.

        :param partition: values defining a partition
        :type partition: list
        :param tbl: table name - defaults to 'target_table_name' job argument
        :type tbl: str, optional
        :return: partition definition
        :rtype: dict
        '''

        glue_tbl = tbl or self.args['target_table_name']
        try:
            return self.glue_api.get_partition(
                DatabaseName = self.args['target_database'],
                TableName = glue_tbl,
                PartitionValues = partition
            )

        except (self.glue_api.exceptions.AccessDeniedException, self.glue_api.exceptions.EntityNotFoundException):
            log.warning("Partition not found: %s", partition)
        return {}


    def create_table(self, tbl: str = None) -> dict:
        '''Create datalake table; return the create table response.
        Database is assumed to be the 'target_database' job argument.

        :param tbl: table name - defaults to 'target_table_name' job argument
        :type tbl: str, optional
        :return: create table response
        :rtype: dict
        '''

        ## Create glue table object
        glue_db      = self.args['target_database']
        glue_tbl     = tbl or self.args['target_table_name']
        glue_tbl_loc = path.join('s3://', self.args['output_bucket'], self.args['output_key'], glue_tbl, '')
        tbl_input    = deepcopy(TABLE_DEF) # deep copy so we don't modify the original TABLE_DEF
        log.trace("TABLE_DEF: %s", tbl_input)

        # Fill in all the "None"s and "e.g."s (i.e. our table's specifics) found in TABLE_DEF:
        tbl_input['Name'] = glue_tbl
        tbl_input['PartitionKeys'] = self.get_partition_keys(glue_tbl, True)
        tbl_input['StorageDescriptor']['Columns'] = [
            # Get column definitions from our schema (excluding partition keys)
            {'Name': fld.name, 'Type': fld.dataType.simpleString()}
            for fld in self.get_schema(glue_tbl).fields
            if fld.metadata.get('purpose') not in {PRPS_PARTITION, PRPS_REDSHIFT_ONLY}
        ]
        tbl_input['StorageDescriptor']['Location'] = glue_tbl_loc
        tbl_input['StorageDescriptor']['SerdeInfo']['Parameters']['path'] = glue_tbl_loc

        resp = self.glue_api.create_table(DatabaseName = glue_db, TableInput = tbl_input)
        log.trace("DEBUG created table %s: %s", glue_tbl, resp)
        assert resp['ResponseMetadata']['HTTPStatusCode'] == 200
        return resp


    def create_partition(self, storage_descriptor: dict, parts: list, tbl: str = None) -> dict:
        '''Create table partition per storage_descriptor and partition arguments
        Database is assumed to be the 'target_database' job argument.

        :param storage_descriptor: storage_descriptor per AWS ``glue.create_partition`` documentation
        :type storage_descriptor: dict
        :param tbl: table name - defaults to 'target_table_name' job argument
        :type tbl: str, optional
        :return: created partition dictionary if we created a partition or Falsey if it already existed
        :rtype: dict
        '''

        glue_tbl = tbl or self.args['target_table_name']
        if self.get_partition(parts, glue_tbl):
            log.info("Partition already exists")
            return {}

        resp = self.glue_api.create_partition(
            DatabaseName = self.args['target_database'],
            TableName = glue_tbl,
            PartitionInput = {
                'Values': parts,
                'StorageDescriptor': storage_descriptor
            }
        )
        assert resp['ResponseMetadata']['HTTPStatusCode'] == 200
        return resp


    def write_dyf(self, dyf, target_path: str, part_keys: list) -> None:
        'Write records to target bucket/folder'

        if self.args.get('profile'):
            print(f"{getattr(dyf, 'name', 'AnonDYF')} dataframe spark plan:")
            dyf.toDF().explain(mode = self.explain_mode)

        self.glue_context.write_dynamic_frame.from_options(
            frame = dyf,
            connection_type = 's3',
            connection_options = {
                'path': target_path,
                'partitionKeys': part_keys
            },
            format = 'parquet'
        )

        log.info("Files containing %s files written to: %s", getattr(dyf, 'name', 'AnonDYF'), target_path)


    def write_reject_dyf(self, etl_state) -> None:
        'Write rejected records to reject target bucket/folder'

        self.write_dyf(
            DynamicFrame.fromDF(etl_state['rejected_df'], self.glue_context, 'Rejected'),
            path.join('s3a://', self.args['rejected_bucket'], self.args['rejected_key']),
            self.get_partition_keys()
        )


    def write_accept_dyf(self, etl_state: dict, target_path: str) -> dict:
        'Write accepted records to database/table'

        dyf = DynamicFrame.fromDF(
            etl_state['dataframe'],
            self.glue_context,
            'Accepted'
        ).resolveChoice(
            choice = 'match_catalog',
            database = self.args['target_database'],
            table_name = self.args['target_table_name']
        )

        self.write_dyf(dyf, target_path, self.get_partition_keys())

        if self.args.get('profile'):
            print('Partition list of dataframe dataframe spark plan:')
            etl_state['dataframe'].select('year', 'month', 'day').distinct().explain(mode = self.explain_mode)

        etl_state['partitions_list'] = [
            [r.year, r.month, r.day]
            for r in etl_state['dataframe'].select('year', 'month', 'day').distinct().collect()
        ]

        log.info("Partition list of dataframe : %s", str(etl_state['partitions_list']))

        return etl_state


    def write_stats_table(self, glue_tbl: str, dict_stats: dict, target_path: str) -> None:
        '''Get effectivity field names as these differ per dataset

        :param glue_tbl: name of stats table
        :type glue_tbl: str
        :param dict_stats: dictionary of data (job_run_id, job_name, input_cnt, etc...)
        :type dict_stats: dict
        :param target_path: s3a location of table in the datalake
        :type target_path: str
        '''

        schema = self.get_schema(glue_tbl)
        data = [dict_stats.get(fld.name) for fld in schema.fields]

        log.info('Data for stats: %s', data)

        df = self.spark.createDataFrame([data], schema)
        self.write_dyf(
            DynamicFrame.fromDF(df, self.glue_context, 'Stats'),
            target_path, self.get_partition_keys(glue_tbl)
        )

    def get_nested_fields(self, df: DataFrame, schema: StructType, prefix: str = None) -> list:
        '''Recursively get field names for nested structures

        :param df: input dataframe
        :type df: dataframe
        :param schema: schema to process
        :type schema: StructType
        :param parent_path: parent path to the current schema
        :type parent_path: str
        :return: list of fields
        :rtype: list[StructField]
        '''
        fields = []

        for fld in schema.fields:
            if fld.metadata.get('purpose') != PRPS_REDSHIFT_ONLY:
                name = prefix + '.' + fld.name if prefix else fld.name

                if isinstance(fld.dataType, StructType):
                    parent_col = prefix if prefix else fld.name
                    # Try to process nested fields
                    nested_fields = self.get_nested_fields(df, fld.dataType, name)
                    struct_fields = struct(*nested_fields).alias(fld.name)
                    try:
                        df.select(struct_fields)
                        fields.append(struct_fields)
                    except:
                        # If processing fails (likely due to null), select just the parent
                        fields.append(col(parent_col).cast(fld.dataType).alias(fld.name))

                elif isinstance(fld.dataType, ArrayType) and isinstance(fld.dataType.elementType, StructType):
                    parent_col = prefix if prefix else fld.name
                    try:
                        # Get all available columns in the source data
                        available_cols = df.select(parent_col).schema[0].dataType.elementType.names
                        array_fields = transform(
                            col(parent_col),
                            lambda x: struct(*[
                                x[f.name].alias(f.name) if f.name in available_cols
                                else lit(None).alias(f.name)
                                for f in fld.dataType.elementType.fields
                            ])
                        ).alias(fld.name)
                        # Try to process the struct elements in array
                        df.select(array_fields)
                        fields.append(array_fields)
                    except:
                        # If processing fails, select just the parent array
                        fields.append(col(parent_col).cast(fld.dataType).alias(fld.name))
                else:
                    # For non-nested fields, just reference the column
                    fields.append(col(name).cast(fld.dataType).alias(fld.name if prefix else name))

        log.info('Fields: %s', fields)

        return fields


    def add_missing_columns(self, df: DataFrame, default_value = None) -> DataFrame:
        '''Select all fields and cast them using the provided schema

        :param df: input dataframe
        :type df: dataframe
        :param tbl: table name - defaults to 'target_table_name' job argument
        :type tbl: str, optional
        :raises KeyError: if **tbl** is not a valid table name in a schema
        :param default_value: default value replacement - defaults to null
        :type default_value: object, optional
        :return: updated dataframe
        :rtype: dataframe
        '''

        glue_tbl = self.args['target_table_name'] \
        if [opt for opt in self.args.keys() if 'redshift' in opt] \
        else self.tbl_metadata.get('parent')

        schema = self.get_schema(glue_tbl)

        df = df.withColumns({fld.name: lit(default_value)
            for fld in schema.fields
            if fld.name not in df.columns and fld.metadata.get('purpose') != PRPS_REDSHIFT_ONLY}
        )

        df = df.select(*self.get_nested_fields(df, schema))

        return df


    def transform_df(self, etl_state: dict) -> dict:
        '''The "meat" of the transformation. Use the schema to copy data from source dataframe to a transformed dataframe.
        The output dataframe has ErrorReason* columns indicating transformation success/failure per column.

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state ('dataframe' = transformed dataframe)
        :rtype: dict
        '''

        log.trace("Transforming with dict: %s", etl_state)
        schema = self.get_schema(self.args['target_table_name'])

        if extract_filter := self.tbl_metadata.get('extract_filter'):
            # The table metadata requests to filter the dataframe right after extract
            df = extract_filter(self, etl_state['dataframe'])
        else:
            df = etl_state['dataframe']

        #Run preproc if present in column's metadata
        for fld in schema.fields:
            if preproc := fld.metadata.get('preproc'):
                df = preproc(self, df)
                if self.args.get('profile'):
                    print('preproc-ed field', fld.name, ':')
                    df.printSchema()
                    df.show(self.trace_show_cnt, truncate = self.trace_show_truncate, vertical = self.trace_show_vertical)
            # end: if preproc
        # end: for fld

        # Use the schema to copy from source_df[field.metadata.path] to dest_df[field.name] (i.e. perform source-to-target)
        s2t_exprs = [
            # Left pad value if 'lpad' is part of the field's metadata
            (lpad(tgt_val.cast(StringType()), *pad_spec) if pad_spec else tgt_val).alias(fld.name)

            for fld in schema.fields

            # Only transform fields that live in the datalake (i.e. *not* redshift only fields)
            if fld.metadata.get('purpose') != PRPS_REDSHIFT_ONLY
            # Copy source field to target field (calling a lambda if provided)
            if (tgt_val := (fld.metadata['path'](self) if callable(fld.metadata['path']) else col(fld.metadata['path']))) is not None
            # Get the left pad spec, if any #pylint: disable=condition-evals-to-constant # this is always true on purpose
            if (pad_spec := fld.metadata.get('lpad')) or True
        ]
        log.debug('s2t_exprs: %s', s2t_exprs)

        df = df.select(*s2t_exprs)

        if self.args.get('profile'):
            print('Peeled records w/ json added:')
            df.printSchema()
            df.show(self.trace_show_cnt, truncate = self.trace_show_truncate, vertical = self.trace_show_vertical)

        # And finally, fill in default values where provided
        default_vals = {
            fld.name: fld.metadata['default']
            for fld in schema.fields
            if 'default' in fld.metadata
        }

        if default_vals:
            log.trace('default_vals: %s', default_vals)
            df = df.na.fill(default_vals)

        # Eliminate true duplicates to evaluate pk uniqueness for validate_df
        if self.tbl_metadata.get('remove_dupes'):
            unique_df = df.groupBy(
                *[fld.name for fld in schema.fields if fld.metadata.get('purpose') not in [PRPS_PARTITION,PRPS_REDSHIFT_ONLY]]
            ).agg(
                cmin(struct(*[col(fld).alias(fld) for fld in self.get_partition_keys()])).alias('min_struct')
            ).cache()
            for fld in self.get_partition_keys():
                unique_df = unique_df.withColumn(fld, col('min_struct')[fld])
            unique_df = unique_df.drop('min_struct')
            etl_state['dupe_count'] =  df.count() - unique_df.count()
            etl_state['dataframe'] = unique_df
            self.metrics.add('dupe_count', etl_state['dupe_count'], MetricsUtil.COUNT)

        else:
            etl_state['dataframe'] = df

        if self.args.get('profile'):
            print('Final transform:')
            df.printSchema()
            df.show(self.trace_show_cnt, truncate = self.trace_show_truncate, vertical = self.trace_show_vertical)

        return etl_state


    def validate_df(self, etl_state: dict) -> dict:
        '''Validate transformed dataframe using AWS DQ library; auto generate rules and allow for custom rules
        via metadata

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''

        def null_or_rule(c: 'DataType', rule_str: str) -> str:
            '''Prefixes a rule to allow null values on nullable fields and no prefix for non-nullable fields.  I.e.
                * For nullable: (ColumnValues "col_nm" = NULL) or (ColumnLength "col_nm" <= 10) # passes on NULL value
                * For non-nullable: (ColumnLength "col_nm" <= 10) # fails on NULL value
            '''
            return f'(ColumnValues "{c.name}" = NULL) or ({rule_str})' if c.nullable else rule_str


        rules = []

        for c in [f for f in self.get_schema(self.args['target_table_name']).fields if f.metadata.get('purpose') != PRPS_REDSHIFT_ONLY]:
            if not c.nullable:
                rules.append(f'IsComplete "{c.name}"')

            if c.metadata and (typ := c.metadata.get('type')) and (typ := typ.capitalize()) != 'String':
                # Determine data type
                data_type, *sz = typ.strip().split('(')
                bounds = None

                if 'Integer' == data_type:
                    bounds = ('-2147483649', '2147483648')
                elif 'Bigint' == data_type:
                    data_type = "Integer"
                    # java can't represent numbers big enough to be out of bounds ('-9223372036854775809', '9223372036854775808').
                    # If a BIGINT is out of bounds then the cast to Integer will fail; so bounds *are* checked.
                elif 'Smallint' == data_type:
                    data_type = "Integer"
                    bounds = ('-32769', '32768')
                elif 'Decimal' == data_type:
                    if sz:
                        # Extract precision and scale from the size parameter
                        params = sz[0].rstrip(')').split(',')
                        if len(params) == 2:
                            precision, scale = map(int, params)
                            data_type = f"Decimal({precision},{scale})"
                        else:
                            data_type = "Decimal"  # Default if no precision/scale specified

                rules.append(null_or_rule(
                    c, f'ColumnLength "{c.name}" <= {sz[0][:-1]}' if data_type in {'Char', 'Varchar'} else
                       f'ColumnDataType "{c.name}" = "{data_type}"'
                ))

                if bounds:
                    # NOTE: DQ 'between' operator is exclusive (doesn't include the endpoints), so
                    # your lower bound needs to be 1 less than the first acceptable value; and
                    # your upper bound needs to be 1 more than the last acceptable value.
                    rules.append(null_or_rule(
                        c, f'ColumnValues "{c.name}" between {bounds[0]} and {bounds[1]}'
                    ))
                # end: if 'String' != data_type

        # end: for c in [...]

        if self.tbl_metadata.get('remove_dupes') and (keys := self.get_keys({KEYS_FOREIGN, KEYS_PRIMARY})):
            rules.append('Uniqueness "' + ', '.join(keys) + '" = 1.0')
        log.trace("Validation rules: %s", rules)

        if not rules:
            log.warning("No validation rules found, so no DQ data will be processed")
            etl_state['rejected_count'] = 0
        else:
            edq_process_rows = EvaluateDataQuality().process_rows(
                frame = DynamicFrame.fromDF(etl_state['dataframe'], self.glue_context, 'Extracted'),
                ruleset = f"Rules = [{','.join(rules)}]",
                publishing_options = {
                    'dataQualityEvaluationContext': 'edq_process_rows',
                    'enableDataQualityCloudWatchMetrics': True,
                    'enableDataQualityResultsPublishing': True,
                },
                additional_options = {
                    'performanceTuning.caching': 'CACHE_NOTHING',
                    'observations.scope': 'ALL',
                    'compositeRuleEvaluation.method': 'ROW'
                }
            )
            log.info("EvaluateDataQuality().process_rows executed")


            row_level_outcomes_df = SelectFromCollection.apply(
                dfc = edq_process_rows,
                key = 'rowLevelOutcomes',
                transformation_ctx = 'dyf_row_level_outcomes'
            ).toDF()
            log.info("SelectFromCollection.apply:rowLevelOutcomes executed")

            etl_state['dataframe'] = row_level_outcomes_df.filter(col('DataQualityEvaluationResult') == "Passed").drop(
                # Accepted data doesn't need to save any DQ columns; it all passed.
                'DataQualityEvaluationResult', 'DataQualityRulesFail', 'DataQualityRulesPass', 'DataQualityRulesSkip'
            ).repartition(*self.get_partition_keys())

            # Reject dataframe won't be transformed, so now is a good time to cache it so 'count' and 'collect' are not recomputed
            etl_state['rejected_df'] = row_level_outcomes_df.filter(col('DataQualityEvaluationResult') != "Passed")\
                .repartition(*self.get_partition_keys()).cache()
            etl_state['rejected_count'] = etl_state['rejected_df'].count()
            etl_state['warning_str'] = dumps(etl_state['rejected_df'].select(col('DataQualityRulesFail')).distinct().collect())

        etl_state['accepted_count'] = etl_state['dataframe'].count()

        return etl_state


    # ETL methods:
    def extract(self, etl_state: dict) -> dict:
        '''Extract data from curated/datalake source

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state ('dataframe')
        :rtype: dict
        '''

        try:
            #Read source data from catalog
            if self.args.get('replay_path', '')[:3] == 's3:':
                log.info("Reading from replay path")
                if self.args['replay_format'] in ('parquet','avro'):
                    df = self.spark.read.format(self.args['replay_format']).load(self.args['replay_path'])
                elif self.args['replay_format'] == 'json':
                    df = self.spark.read.schema(StructType([
                        StructField(fld.name, fld.dataType, fld.nullable)
                        for fld in self.get_schema().fields
                        if fld.name not in self.tbl_metadata.get('replay_calc_flds', [])
                        ])).json(self.args['replay_path'])

            elif self.args.get('reprocess_from'):
                log.info("Reading source data from catalog for reprocess")
                df = self.glue_context.create_dynamic_frame.from_catalog(
                    format_options = {'attachFilename': 's3_object'},
                    database = self.args['source_database'],
                    table_name = self.args['source_table_name'],
                    push_down_predicate = AWSGLueJob.get_default_push_down_predicate(
                        datetime.strptime(self.args['reprocess_from'], '%Y-%m-%d'),
                        datetime.strptime(self.args['reprocess_to'], '%Y-%m-%d'),
                        int(self.args.get('pushdown_predicate_histry_days', self.conf.get('predicate', {}).get('history', 30))),
                        int(self.args.get('pushdown_predicate_future_days', self.conf.get('predicate', {}).get('future',   2)))
                    )
                ).toDF()

            else:
                log.info("Reading source data from catalog using bookmarks")
                df = self.glue_context.create_dynamic_frame.from_catalog(
                    format_options = {'attachFilename': 's3_object'},
                    database = self.args['source_database'],
                    table_name = self.args['source_table_name'],
                    transformation_ctx = self.args['target_table_name'],
                    push_down_predicate = AWSGLueJob.get_default_push_down_predicate(
                        history_days = int(self.args.get('pushdown_predicate_histry_days', self.conf.get('predicate', {}).get('history', 30))),
                        future_days = int(self.args.get('pushdown_predicate_future_days', self.conf.get('predicate', {}).get('future',   2)))
                    )
                ).toDF()

            if 's3_object' not in df.columns:
                # 'from_options' supports 'attachFilename', 'from_catalog' may not. 'input_file_name' returns a datalake/bookmark filename:
                # e.g. 'datalake-id-stuff-m-r://datalake-us-east-1-dev1-raw-secure/com/folders/tbls/year=2024/..parts../filename-fb877d953cf6'
                df = df.withColumn(
                    's3_object', # Strip the leading 'protocol' portion and replace with 's3' protocol:
                    concat(lit('s3://'), split(input_file_name(), '://').getItem(1))
                )

            if self.tbl_metadata.get('add_missing_columns'):
                df = self.add_missing_columns(df)

            etl_state['dataframe'] = df
            etl_state['source_count'] = df.count()

            self.metrics.add('source_count', etl_state['source_count'], MetricsUtil.COUNT)

            if self.args.get('profile'):
                print('Source schema/records:')
                df.printSchema()
                df.show(self.trace_show_cnt, truncate = self.trace_show_truncate, vertical = self.trace_show_vertical)

        except Exception:
            log.exception('Failed to read from raw')
            raise

        return etl_state


    def transform(self, etl_state: dict) -> dict:
        '''Transform source dataframe as per schema def

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''

        if 0 == etl_state['source_count']:
            log.warning("Source dataframe has no data/columns to transform")

        else:
            try:
                etl_state = super().transform(etl_state)
                etl_state = self.transform_df(etl_state)
                etl_state = self.validate_df(etl_state)

            except Exception:
                log.exception('Error occured while transforming/validating dataframe')
                raise

        return etl_state


    def load(self, etl_state: dict) -> dict:
        '''Write data to target

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''

        target_path = path.join('s3a://', self.args['output_bucket'], self.args['output_key'], '')

        if 0 == etl_state['source_count']:
            log.warning("Source dataframe has no data/columns to load")
        else:
            etl_state = super().load(etl_state)

            if 0 != etl_state['rejected_count']:
                self.write_reject_dyf(etl_state)

            if 0 != etl_state['accepted_count']:
                table_path = path.join(target_path, self.args['target_table_name'])
                etl_state = self.write_accept_dyf(etl_state, table_path)

                log.info("Updating table: %s metadata with list of new partitions.", self.args['target_table_name'])

                table = self.get_table(self.args['target_table_name'])
                log.trace("get_table definition: %s", table)

                partition_keys     = self.get_partition_keys()
                storage_descriptor = table['Table']['StorageDescriptor'].copy()
                log.info("check if partition exists")

                for partition_list in etl_state['partitions_list']:
                    log.info("Updating partition: %s", partition_list)

                    partition_path = path.join(*[f"{key}={value}" for key, value in zip(partition_keys, partition_list)])
                    storage_descriptor['Location'] = path.join(table_path, partition_path)
                    log.debug("storage_descriptor Loc: %s", storage_descriptor['Location'])

                    self.create_partition(storage_descriptor, partition_list)
                    log.info("Partition Successfully created")
            # end: if valid_records

            if self.args.get('profile'):
                print('load spark plan:')
                etl_state['dataframe'].explain(mode = self.explain_mode)

        # Write stats table
        stats_dict = {
            'orc_run_id': self.args['orc_run_id'],
            'target_table': self.args['target_table_name'],
            'job_run_id': self.args['JOB_RUN_ID'],
            'job_name': self.args['JOB_NAME'],
            'job_runtime': int(time()),
            'input_cnt': etl_state.get('source_count', 0),
            'output_cnt': etl_state.get('accepted_count', 0),
            'reject_cnt': etl_state.get('rejected_count', 0),
            'dupe_cnt': etl_state.get('dupe_count', 0),
            'warning_str': etl_state.get('warning_str', '').replace('[]', '')
        }

        stats_tbl_name = self.get_table_with_type('stats')
        table_path = path.join(target_path, stats_tbl_name)
        self.write_stats_table(stats_tbl_name, stats_dict, table_path)

        table = self.get_table(stats_tbl_name)
        log.trace("get_table definition: %s", table)

        partition_keys     = self.get_partition_keys(stats_tbl_name)
        storage_descriptor = table['Table']['StorageDescriptor'].copy()

        partition_list = [stats_dict[key] for key in partition_keys]
        partition_path = path.join(*[f"{key}={parse.quote(value)}" for key, value in zip(partition_keys, partition_list)])

        storage_descriptor['Location'] = path.join(table_path, partition_path)

        self.create_partition(storage_descriptor, partition_list, stats_tbl_name)

        return etl_state


    def commit(self, etl_state: dict) -> dict:
        '''Commit AWSGLueJob state (including AWS Glue bookmarks)

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''

        super().commit(etl_state)
        if self._glue_client is None:
            log.warning('GlueClient connection never established')
            return etl_state

        self._glue_client.commit(etl_state)
        return etl_state


########################################

TABLE_DEF = {
    'Owner': 'spark', # or hadoop, etc...
    'Name': None, # e.g. example_app_curated
    'PartitionKeys': None, # e.g. [{'Name': 'year', 'Type': 'string'}, ... {'Name': 'day', 'Type': 'string'}]
    'StorageDescriptor': {
        'Columns': None, # e.g. [{'Name': 'eventname', 'Type': 'string'}, ... {'Name': 's3_object', 'Type': 'string'}]
        'Location': None, # e.g. 's3://datalake-us-east-1-dev1-curated-secure/example/entp-app/example-app-folder/example_app_curated'
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'Parameters': {'serialization.format': '1', 'path': None} # e.g. s3://curated-secure/domain/entp-xmpl/domain/example_app_curated
        },
        'BucketColumns': [],
        'Compressed': False,
        'NumberOfBuckets': -1,
        'Parameters': {},
        'SkewedInfo': {
            'SkewedColumnNames': [],
            'SkewedColumnValueLocationMaps': {},
            'SkewedColumnValues': [],
        },
        'SortColumns': [],
        'StoredAsSubDirectories': False
    },
    'TableType': 'EXTERNAL_TABLE',
    'Retention': 0,
    'Parameters': {
        'EXTERNAL': 'TRUE',
        'compressionType': 'snappy',
        'classification': 'parquet'
    }
}
