from setuptools import setup

setup(name='disqueue',
      version='0.1',
      description='Client library for distributed queue',
      url='https://github.com/chandrapavansai/distributed-queue',
      license='MIT',
      packages=['disqueue'],
      install_requires=[
          'requests',
      ],
      zip_safe=False)
