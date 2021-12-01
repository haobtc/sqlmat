from typing import Tuple, List, Optional
import json
import sys
import os
import shlex
import asyncio
import argparse
import logging
import tempfile
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def find_sqlmat_json() -> Optional[dict]:
    json_path = os.getenv('SQLMAT_JSON_PATH')
    if json_path:
        with open(json_path) as f:
            cfg = json.load(f)
            return cfg

    # iterate through the current dir up to the root dir "/" to find a
    # .sqlmat.json
    workdir = os.path.abspath(os.getcwd())
    while workdir:
        json_path = os.path.join(workdir, '.sqlmat.json')
        if os.path.exists(json_path):
            with open(json_path) as f:
                cfg = json.load(f)
                return cfg
        parentdir = os.path.abspath(os.path.join(workdir, '..'))
        if parentdir == workdir:
            break
        workdir = parentdir
    logger.warning('fail to find .sqlmat.json')
    return None

def find_dsn(prog: str, desc: str) -> Tuple[str, List[str]]:
    parser = argparse.ArgumentParser(
        prog=prog,
        description=desc)
    parser.add_argument('-d', '--dsn',
                        type=str,
                        help='postgresql dsn')

    parser.add_argument('-g', '--db',
                        type=str,
                        default='default',
                        help='postgresql db instance defined in .sqlmat.json')

    parser.add_argument('callee_args',
                        type=str,
                        nargs='*',
                        help='command line arguments of callee programs')

    # from arguments
    args = parser.parse_args()
    if args.dsn:
        return args.dsn, args.callee_args

    # find dsn from ./.sqlmat.json
    cfg = find_sqlmat_json()
    if cfg:
        dsn = cfg['databases'][args.db]['dsn']
        assert isinstance(dsn, str)
        return dsn, args.callee_args

    # default dsn using username
    user = os.getenv('USER', '')
    default_dsn = f'postgres://{user}@127.0.0.1:5432/{args.db}'

    logger.warning('no postgres dsn specified, use %s instead', default_dsn)
    return default_dsn, args.callee_args

def joinargs(callee_args: List[str]) -> str:
    if hasattr(shlex, 'join'):
        return shlex.join(callee_args)
    else:
        return ' '.join(shlex.quote(a) for a in callee_args)

# run psql client
async def run_shell(dsn: str, callee_args: List[str]) -> None:
    p = urlparse(dsn)
    username = p.username or ''
    password = p.password or ''
    dbname = p.path[1:]
    hostname = p.hostname
    port = p.port or 5432

    temp_pgpass = tempfile.NamedTemporaryFile(mode='w')
    print(
        '{}:{}:{}:{}:{}'.format(hostname, port, dbname, username, password),
            file=temp_pgpass,
            flush=True)
    os.environ['PGPASSFILE'] = temp_pgpass.name
    command = 'psql -h{} -p{} -U{} {} {}'.format(hostname, port, username, joinargs(callee_args), dbname)
    proc = await asyncio.create_subprocess_shell(command)
    await proc.communicate()

def cl_run_shell() -> None:
    dsn, callee_args = find_dsn('sqlmat-shell', 'run psql client shell')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_shell(dsn, callee_args))

# run dbdump
async def run_dbdump(dsn: str, callee_args: List[str]) -> None:
    p = urlparse(dsn)
    username = p.username or ''
    password = p.password or ''
    dbname = p.path[1:]
    hostname = p.hostname
    port = p.port or 5432

    temp_pgpass = tempfile.NamedTemporaryFile(mode='w')
    print(
        '{}:{}:{}:{}:{}'.format(hostname, port, dbname, username, password),
        file=temp_pgpass,
        flush=True)
    os.environ['PGPASSFILE'] = temp_pgpass.name
    command = 'pg_dump -h{} -p{} -U{} {} {}'.format(hostname, port, username, joinargs(callee_args), dbname)
    proc = await asyncio.create_subprocess_shell(command)
    await proc.communicate()

def cl_run_dbdump() -> None:
    dsn, callee_args = find_dsn('sqlmat-dump', 'dump database')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_dbdump(dsn, callee_args))

# generate alembic migrations
def gen_migrate(dsn: str) -> None:
    init_data = ALEMBIC_INIT.replace('{{dsn}}', dsn)
    with open('alembic.ini', 'w') as f:
        f.write(init_data)

def cl_gen_migrate() -> None:
    dsn, callee_args = find_dsn('sqlmat-genmigrate', 'generate alembic migration')
    gen_migrate(dsn)
    print('Wrote alembic.ini')

ALEMBIC_INIT = '''\
# A generic, single database configuration.

[alembic]
# path to migration scripts
script_location = migrations

# template used to generate migration files
# file_template = %%(rev)s_%%(slug)s

# timezone to use when rendering the date
# within the migration file as well as the filename.
# string value is passed to dateutil.tz.gettz()
# leave blank for localtime
# timezone =

# max length of characters to apply to the
# "slug" field
#truncate_slug_length = 40

# set to 'true' to run the environment during
# the 'revision' command, regardless of autogenerate
# revision_environment = false

# set to 'true' to allow .pyc and .pyo files without
# a source .py file to be detected as revisions in the
# versions/ directory
# sourceless = false

# version location specification; this defaults
# to migrations/versions.  When using multiple version
# directories, initial revisions must be specified with --version-path
# version_locations = %(here)s/bar %(here)s/bat migrations/versions

# the output encoding used when revision files
# are written from script.py.mako
# output_encoding = utf-8

#sqlalchemy.url = driver://user:pass@localhost/dbname
sqlalchemy.url = {{dsn}}

# Logging configuration
[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S

'''
