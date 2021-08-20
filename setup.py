from setuptools import setup

setup(name='cauldron',
      version='1.5.5',
      author='Ankit Chandawala',
      author_email='ankitchandawala@gmail.com',
      url='https://github.com/nerandell/cauldron',
      description='Utils to reduce boilerplate code',
      packages=['cauldron'],
      install_requires=[
          'aiopg==0.16.0',
          'aioredis==0.2.9',
          'psycopg2==2.8.4',
          'elasticsearch==7.6.0'
      ])
