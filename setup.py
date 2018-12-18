from setuptools import setup

setup(name='stuff',
      version='0.01',
      packages=['stuff'],
      description='Random stuff',
      install_requires=[
          'aiohttp'
      ],
      entry_points={
          'console_scripts': [
              'reddit = stuff.reddit:main'
          ]
      })
