from setuptools import setup

setup(name='vessel',
      version='3.0.0',
      author='1mg',
      author_email='devops@1mg.com',
      url='https://github.com/1mgOfficial/vessel',
      description='Utils to reduce boilerplate code',
      packages=['vessel'], install_requires=['aiopg', 'aioredis', 'psycopg2'])
