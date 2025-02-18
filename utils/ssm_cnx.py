import subprocess,sys

# implement pip as a subprocess:
#subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'snowflake-connector-python[pandas]'])


import boto3
import snowflake.connector

def get_ssm_object():
    session = boto3.Session(region_name='us-west-2')
    return boto3.client('ssm', region_name='us-west-2')


def get_snowflake_connection_parameter(param_name, ssm=None, env='prod-ds'):
    if ssm is None:
        ssm = get_ssm_object()
    snowflake_param_name = (
        '/nmg/cdp/{e}/snowflake/{p}'.format(e=env.strip(), p=param_name)
    )
    d = ssm.get_parameter(Name=snowflake_param_name, WithDecryption=True)
    return d['Parameter']['Value']


def get_snowflake_connection_param_dict(
    dbname='NMEDWPRD_DB', env='prod-ds', ssm=None
):
    if ssm is None:
        ssm = get_ssm_object()
    d = {}
    d['user'] = get_snowflake_connection_parameter('user', ssm, env)
    d['password'] = get_snowflake_connection_parameter('password', ssm, env)
    d['account'] = get_snowflake_connection_parameter('account', ssm, env)
    d['database'] = dbname
    d['warehouse'] = get_snowflake_connection_parameter('warehouse', ssm, env)
    return d


def get_snowflake_connection_from_param_dict(d):
    return snowflake.connector.connect(
        user=d['user'],
        password=d['password'],
        account=d['account'],
        database=d['database']
    )


def get_snowflake_connection(env='prod-ds', dbname='NMEDWPRD_DB', ssm=None):
    if ssm is None: ssm = get_ssm_object()
    d = get_snowflake_connection_param_dict(dbname, env, ssm)
    return get_snowflake_connection_from_param_dict(d)


