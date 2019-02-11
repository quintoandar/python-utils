from setuptools import setup, find_packages

setup(
    name='qa-python-utils',
    version='0.1.0',
    packages=find_packages(),
    url='https://github.com/quintoandar/python-utils',
    license='',
    author='QuintoAndar',
    author_email='enishime@quintoandar.com.br',
    description='Package for python utils',
    install_requires=[open('requirements.txt').read().strip().split('\n')]
)
