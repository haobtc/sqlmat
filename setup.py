from distutils.core import setup
from setuptools import find_packages

setup(name='sqlmat',
      version='0.1.7',
      description='simply map python3 statement to postgresql statement',
      author='Zeng Ke',
      author_email='superisaac.ke@gmail.com',
      packages=find_packages(),
      tests_require = [
          'pytest',
          'pytest-asyncio'
      ],
      install_requires=[
          'asyncpg>=0.21.0'
      ]
)

