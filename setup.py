from setuptools import setup, find_packages

setup(name='trelliolibs',
      version='0.37.2',
      author='Abhishek Verma, Nirmal Singh',
      author_email='ashuverma1989@gmail.com, nirmal.singh.cer08@itbhu.ac.in',
      url='https://github.com/zoniclabs/trelliolibs',
      description='Utilities for trellio framework',
      packages=find_packages(),
      install_requires=['trellio', 'trelliopg', 'cerberus'])
