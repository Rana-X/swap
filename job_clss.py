'''Module provides ETL loading of Data Lake/Redshift table(s)'''

from abc import ABC, abstractmethod
from importlib import import_module
from os import sep
from time import time

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession

from glue_utils import get_logger, Logger, TRACE, MetricsUtil, RC

log: Logger = None
ETL_NAMESPACE = 'ETL Jobs'


# ============================================================

class AWSJob(ABC):
    '''AWSJob is the base class for all types of jobs ran within an AWS context

    :param argv: list of command line parameters (usually sys.argv)
    :type argv: list
    :param req: list of required command line parameters
    :type req: list
    :param optnl: list of optional command line parameters
    :type optnl: list, optional
        Exit status >= RC.ERROR indicates an error
    '''

    args: list
    conf: dict


    #pylint: disable=dangerous-default-value # the empty list seems like a fine default
    def __init__(self, argv: list, req: list, optnl: list = []):
        'Constructor method'

        self.args = getResolvedOptions(
            argv,
            # Search for optional params in argv list and resolve those supplied:
            req + list({p[2:] for p in argv if p.startswith('--')}.intersection(optnl))
        )

        globals()['log'] = get_logger(self.args['JOB_NAME'], self.args['JOB_NAME'])
        self.metrics = MetricsUtil(self.args['JOB_NAME'], ETL_NAMESPACE)

        for k, v in self.args.items():
            if k.endswith('_dir') or k.endswith('_key'):
                # Enforce a standard, arguments like '*_dir' or '*_key' must end with a slash (AWS often requires this)
                assert v.endswith(sep)

        # If 'profile' is True then extra info is printed like: profiling, printSchema(), show(), etc...
        self.args['profile'] = self.args.get('profile', log.isEnabledFor(TRACE)) in ('true', True)
        self.enable_custom_metrics = 'true' == self.args.get('enable_custom_metrics', 'False').lower()

        for k, v in self.args.items():
            log.info("CMDLINE-ARG %s: %s", k, v)

        if 'configs' in self.args:
            self.conf = (import_module(self.args['configs'])).settings.copy()
            log.debug("Configs loaded %s", self.conf)
        else:
            self.conf = None
            log.debug('No configs loaded')

        log.debug('AWSJob constructed :)')


    # ETL methods:
    def connect(self, etl_state: dict) -> dict:
        '''Connect to resources necessary to complete the ETL job

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state (added 'job_start_time')
        :rtype: dict
        '''

        etl_state['job_start_time'] = time()
        return etl_state


    @abstractmethod
    def extract(self, etl_state: dict) -> dict:
        '''Extract data into a Transform ready state

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''
        return etl_state


    @abstractmethod
    def transform(self, etl_state: dict) -> dict:
        '''Transform and Validate input data into an Load ready state

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''
        return etl_state


    @abstractmethod
    def load(self, etl_state: dict) -> dict:
        '''Load output data into the target system

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''
        return etl_state


    def commit(self, etl_state: dict) -> dict:
        '''Commit job in its context

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: etl_state
        :rtype: dict
        '''

        now = time()

        self.metrics.add(
            'JobRunTime',
            now - etl_state['job_start_time'],
            MetricsUtil.SECONDS
        )

        self.metrics.commit()
        return etl_state


    def create_etl_state(self) -> dict:
        '''Create an 'etl_state' variable for a tranformation job

        :return: 'etl_state' that tracks ETL specific states
        :rtype: dict
        '''
        return {'return_code': RC.OK}


    def run_job(self, **kwargs) -> dict:
        '''Run a tranformation job thru connect, extract, transform, load, and commit stages

        :param \\**kwargs: **methods** (list[callable]), optional -- overrides default job steps [connect, extract, transform, load, and commit]
        :return: updated etl_state
        :rtype: dict
        '''

        etl_state = kwargs.get('etl_state', self.create_etl_state())

        for f in kwargs.get('methods', [self.connect, self.extract, self.transform, self.load, self.commit]):
            etl_state = f(etl_state)
            log.trace("main loop, after: %s; etl_state: %s", f, etl_state)

            if etl_state['return_code'] is not RC.OK:
                # Log either an error or warning based on 'return_code'
                (log.error if etl_state['return_code'] >= RC.ERROR else log.warn)("'%s' job state exiting: %s", f, etl_state)
                break

        # TODO run 'commit' here instead of the loop; run on warnings, probably on errors too?  So we can commit metrics, etc...
        return etl_state


# ============================================================

class GlueClient:
    '''GlueClient handles interacting with AWS Glue (it is not a *Job* class)

    :param sql_context: list of command line parameters (usually sys.argv)
    :type argv: class: `SQLContext`
    :param args: dictionary of command line parameters
    :type req: dict
    '''

    glue_context: GlueContext
    job: Job


    def __init__(self, sql_context: SQLContext, args: dict):
        'Constructor method'

        self.glue_context = GlueContext(sql_context)
        self.job = Job(self.glue_context)
        self.job.init(args['JOB_NAME'], args)
        log.debug('GlueClient constructed :)')


    def __del__(self):
        'GlueClient destructor: delete the glue context and AWS job attributes'
        del self.glue_context
        del self.job
        log.debug('GlueClient destructed :)')


    @classmethod
    def connect(cls, sparkContext, args):
        'Create SparkClient'

        glue_client = GlueClient(sparkContext, args)
        log.debug("Connected to glue_client=%s", glue_client)
        return glue_client


    def commit(self, etl_state: dict) -> dict:
        '''Commit AWS Job state

        :param etl_state: state important to the ETL process
        :type etl_state: dict
        :return: updated etl_state
        :rtype: dict
        '''

        if self.job is None:
            log.error('GlueClient connection to AWS failed')
            etl_state['return_code'] = RC.ERR_AWS_ERROR
            return etl_state
        self.job.commit()
        return etl_state


# ============================================================

class SparkClient:
    '''SparkClient handles interacting with Spark sessions (it is not a *Job* class)

    :param conf: spark configuration (likely from ``gluejobs/conf/configs.py``)
    :type conf: class: `SparkConf`
    '''

    spark: SparkSession


    #pylint: disable=dangerous-default-value # the empty list seems like a fine default
    def __init__(self, conf):
        'Constructor method'

        log.info('SparkClient connecting to resources:')
        log.trace("spark config: %s", conf)
        self.spark = SparkSession.builder.config(conf = conf).getOrCreate()
        log.debug("SparkClient constructed, connected to spark=%s", self.spark)


    def __del__(self):
        'SparkClient destructor: delete the spark session'
        del self.spark
        log.debug('SparkClient destructed :)')


    @classmethod
    def connect(cls, conf):
        'Create SparkClient'

        spark = SparkClient(conf)
        log.debug("Connected to glue_client=%s", spark)
        return spark
