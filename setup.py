from setuptools import setup, find_packages

setup(
  name='influxdb-nigma',
  version='0.0.1',
  description='Object Based InfluxDB Query Running and Generation',
  author='Edgar Uribe',
  author_email='edgaruribe369@gmail.com',
  url='https://github.com/edgaruribe369/influxdb-nigma',
  packages=find_packages(),
  scripts=[],
  data_files=[],
  # keep requires as light as possible to allow easy importing
  install_requires=[],
  tests_require=['influxdb'],
  test_suite='__main__.discoverTests',
)