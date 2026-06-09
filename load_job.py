'''Application provides ETL operations from curation to datalake to redshift'''

from sys import argv
from importlib import import_module

from glue_utils import get_logger, Logger, MetricsUtil, RC
from glue_clss import AWSGLueJob

log: Logger = None


class LoadJob(AWSGLueJob):
    '''LoadJob is an `AWSGLueJob` specialized to ETL from curated to datalake

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


    # ETL methods:
    def connect(self, etl_state: dict) -> dict:
        '''Connect to datalake and ensure assets (checkpoint dir and tables) exist

        :param self: class instance
        :type self: class: `LoadJob`
        :param etl_state: state important to the ETL process
        :type etl_state: dict

        :return: updated etl_state
        :rtype: dict
        '''

        etl_state = super().connect(etl_state)
        if not self.get_table():
            self.create_table()
        if not self.get_table(self.get_table_with_type('stats')):
            self.create_table(self.get_table_with_type('stats'))
        return etl_state


    def transform(self, etl_state: dict) -> dict:
        '''Transform source dataframe as per schema def

        :param self: class instance
        :type self: class: `LoadJob`
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
                etl_state['xformed_count'] = etl_state['dataframe'].count()

            except Exception:
                log.exception('Error occured while transforming/validating dataframe')
                raise

            self.metrics.add('RecordsTransformed', etl_state['xformed_count'], MetricsUtil.COUNT)

        return etl_state


# ============================================================

def main(**kwargs) -> int:
    '''Construct a tranformation job and E-T-L data from input to output

    :param \\**kwargs: **job** (class: `LoadJob`) -- Optional pre-instantiated LoadJob (useful for many reasons including testing)

    :return: status of job run (see RC enum)
    :rtype: int

    :Job Required Args:

    ================== ===========
    Parameter          Description
    ================== ===========
    JOB_NAME           Job name
    configs            configuration parameters; e.g. 'conf.configs'
    checkpoint_dir     datalake storage for job bookmark checkpoints; e.g. 's3://bkt-name/path/table-name/checkpoint/'
    orc_run_id         state machine run id; e.g. 'step-function-run-id-here'
    output_bucket      datalake storage bucket; e.g. 'datalake-us-east-1-dev1-curated-secure'
    output_key         datalake storage folder; e.g. 'example/entp-app/example-app-folder/'
    rejected_bucket    rejected records storage bucket; e.g. 'idfp-us-east-1-dev1-decp-xmpl'
    rejected_key       rejected records storage folder; e.g. 'xmpl/data/example-app-folder-curation/reject/'
    schema             python schema module; e.g. 'example_app_schemas'
    source_database    datalake source database; e.g. 'datalake_dev1_xmpl'
    source_table_name  datalake source table; e.g. 'rawsecure_flight_events'
    target_database    datalake target database; e.g. 'datalake_dev1_entp_xmpl'
    target_table_name  datalake target table; e.g. 'example_app_curated'
    temp_dir           temp storage for cache and other needs; e.g. 's3://bkt-name/path/keys/temporary'
    ================== ===========

    :Job Optional Args:

    ============================== ===========
    Parameter                      Description
    ============================== ===========
    profile                        if set, enable trace logging and `cProfile` analysis
    pushdown_predicate_future_days # of days in the future to filter the partition when bookmarks fail; e.g. '2'
    pushdown_predicate_histry_days # of days in the past to filter the partition when bookmarks fail; e.g. '30'
    replay_format                  format of the rejected records source file(s); e.g. 'parquet' or 'json'
    replay_path                    uri to (rejected) data to replay; e.g. 's3://bkt-name/path/table-name/reject/'
    reprocess_from                 YYYY-MM-DD of pushdown predicate for replay; e.g. '2024-01-02'
    reprocess_to                   YYYY-MM-DD of pushdown predicate for replay; e.g. '2024-01-03'
    ============================== ===========
    '''

    job = kwargs.get('job') or LoadJob(argv, [
        'JOB_NAME',
        'configs',
        'orc_run_id',
        'output_bucket',
        'output_key',
        'rejected_bucket',
        'rejected_key',
        'schema',
        'source_database',
        'source_table_name',
        'target_database',
        'target_table_name',
        'temp_dir'
    ],[
        ## Optional:
        'profile',
        'pushdown_predicate_future_days',
        'pushdown_predicate_histry_days',
        'replay_format',
        'replay_path',
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
