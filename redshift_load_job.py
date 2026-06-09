'''Application provides ETL loading of Redshift Target Tables with CDC'''

from sys import argv
from importlib import import_module

from glue_utils import get_logger, Logger, RC
from redshift_clss import AWSGLueRedshiftJob

log: Logger = None


class LoadJob(AWSGLueRedshiftJob):
    '''LoadJob is an `AWSGLueRedshiftJob` specialized for loading datalake data into Redshift

    :param argv: list of command line parameters (usually sys.argv)
    :type argv: list
    :param req: list of required command line parameters
    :type req: list
    :param optnl: list of optional command line parameters
    :type optnl: list, optional
    '''

    #pylint: disable=redefined-outer-name # using 'argv' b/c that is the essence of it
    #pylint: disable=dangerous-default-value # the empty list seems like a fine default
    def __init__(self, argv: list, req: list, optnl: list = []):
        'Constructor method'

        super().__init__(argv, req, optnl)
        global log #pylint: disable=global-statement # log is a global variable
        log = log or get_logger(self.args['JOB_NAME'], self.args['JOB_NAME'])
        log.debug('LoadJob constructed :)')


# ============================================================

def main(**kwargs) -> int:
    '''Construct a tranformation job and E-T-L data from input to output

    :param \\**kwargs: **job** (class: `LoadJob`) -- Optional pre-instantiated LoadJob (useful for many reasons including testing)

    :return: status of job run (see RC enum)
    :rtype: int

    :Required Job Args:

    ================== ===========
    Parameter          Description
    ================== ===========
    JOB_NAME           Job name
    configs            configuration parameters; e.g. 'conf.configs'
    checkpoint_dir     <add-descr>; e.g. 's3://idfp-us-east-1-dev1-decp-xmpl/xmpl/data/example-app-folder-transform/checkpoint/'
    redshift_iam_role  role with redshift database CRUD permissions
    redshift_secret    name of credentials secret to assume `redshift_iam_role`
    redshift_temp_dir  directory passed to redshift dataframe write(s) (in option ``tempdir``)
    schema             python schema module; e.g. 'example_app_schemas'
    source_database    datalake source database; e.g. 'datalake_dev1_xmpl'
    source_table_name  datalake source table; e.g. 'example_app_cdc_tbl'
    stage_schema       datalake target database; e.g. 'idw_st'
    target_database    datalake target database; e.g. 'swadb'
    target_schema      datalake target database; e.g. 'idw_db'
    target_table_name  datalake target table; e.g. 'example_app_cdc_tbl'
    temp_dir           temp storage for cache and other needs; e.g. 's3://bkt-name/path/keys/temporary'
    ================== ===========

    :Optional Job Args:

    ============================== ===========
    Parameter                      Description
    ============================== ===========
    profile                        if set, enable trace logging and `cProfile` analysis
    pushdown_predicate_future_days <add-descr>; e.g. '2'
    pushdown_predicate_histry_days <add-descr>; e.g. '30'
    reprocess_from                 YYYY-MM-DD of pushdown predicate for replay; e.g. '2024-01-02'
    reprocess_to                   YYYY-MM-DD of pushdown predicate for replay; e.g. '2024-01-03'
    ============================== ===========
    '''

    job = kwargs.get('job') or LoadJob(argv, [
        'JOB_NAME',
        'configs',
        'redshift_iam_role',
        'redshift_secret',
        'redshift_temp_dir',
        'schema',
        'source_database',
        'source_table_name',
        'stage_schema',
        'target_database',
        'target_schema',
        'target_table_name',
        'temp_dir'
    ],[
        ## Optional:
        'profile',
        'pushdown_predicate_future_days',
        'pushdown_predicate_histry_days',
        'reprocess_from',
        'reprocess_to'
    ])

    global log #pylint: disable=global-statement # log is a global variable
    log = get_logger(job.args['JOB_NAME'], job.args['JOB_NAME'])
    if job.args.get('profile'):
        prof = (import_module('cProfile')).Profile()
        etl_state = prof.runcall(job.run_job, **kwargs)
        prof.print_stats(sort = 'cumulative')
    else:
        etl_state = job.run_job()
    return etl_state.get('return_code', RC.ERR_PGM_ERROR)


if __name__ == '__main__':
    main_rc = main()
    if main_rc >= RC.ERROR:
        raise SystemExit(main_rc)
