import sys
import os
import asyncio
import argparse
import logging
import tempfile
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def find_dsn(prog: str, desc: str) -> str:
    parser = argparse.ArgumentParser(
        #prog='sqlmat-genmigrate',
        prog=prog,
        description=desc)
    parser.add_argument('-d', '--dsn',
                        type=str,
                        help='postgresql dsn')

    args = parser.parse_args()
    if args.dsn:
        return args.dsn

    dsn = os.getenv('SQLMAT_DSN')
    if dsn:
        return dsn

    user = os.getenv('USER', '')
    default_dsn = f'postgres://{user}@127.0.0.1:5432/{user}'

    logger.warning('no postgres dsn specified, use %s instead', default_dsn)
    return default_dsn

# run psql client
async def run_shell(dsn: str) -> None:
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
    command = 'psql -h{} -p{} -U{} {}'.format(hostname, port, username, dbname)
    proc = await asyncio.create_subprocess_shell(command)
    await proc.communicate()

def cl_run_shell() -> None:
    dsn = find_dsn('sqlmat-shell', 'run psql client shell')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_shell(dsn))

# run dbdump
async def run_dbdump(dsn: str) -> None:
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
    command = 'pg_dump -h{} -p{} -U{} {}'.format(hostname, port, username, dbname)
    proc = await asyncio.create_subprocess_shell(command)
    await proc.communicate()

def cl_run_dbdump() -> None:
    dsn = find_dsn('sqlmat-dump', 'dump database')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_dbdump(dsn))

# generate alembic migrations
def gen_migrate(dsn: str) -> None:
    init_data = ALEMBIC_INIT.replace('{{dsn}}', dsn)
    with open('alembic.ini', 'w') as f:
        f.write(init_data)

def cl_gen_migrate() -> None:
    dsn = find_dsn('sqlmat-genmigrate', 'generate alembic migration')
    gen_migrate(dsn)
    print('Wrote alembic.ini')

ALEMBIC_INIT = '''\
ation specification; this defaults
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
