from distutils.core import setup
from setuptools import find_packages

setup(name='sqlmat',
      version='0.2.4',
      description='simply map python3 statement to postgresql statement',
      author='Zeng Ke',
      author_email='superisaac.ke@gmail.com',
      packages=find_packages(),
      tests_require = [
          'pytest',
          'pytest-asyncio'
      ],
      install_requires=[
          'asyncpg >= 0.21.0',
          'alembic >= 1.4.2'
      ],
      entry_points={
          'console_scripts': [
              'sqlmat-genmigrate = sqlmat.utils:cl_gen_migrate',
              'sqlmat-shell = sqlmat.utils:cl_run_shell',
              'sqlmat-dump = sqlmat.utils:cl_run_dbdump',
          ],
      },
)

