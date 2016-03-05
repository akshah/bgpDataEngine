from setuptools import setup

setup(name='bgpDataEngine',
      packages=['bgpDataEngine'],
      version='0.1.1',
      description='Modules to manage bgp data sources',
      author='Anant Shah',
      author_email='akshah@rams.colostate.edu',
      install_requires=[
          'netaddr',
          'pymysql',
          'pycurl',
      ],
)
