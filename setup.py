from distutils.core import setup

setup(
    name='qa-python-utils',
    version='0.1.0',
    packages=['qa_python_utils',
              'qa_python_utils.google',
              'qa_python_utils.aws',
              'qa_python_utils.kafka'],
    url='https://github.com/quintoandar/python-utils',
    license='',
    author='QuintoAndar',
    author_email='enishime@quintoandar.com.br',
    description='Package for python utils',
    install_requires=[open('requirements.txt').read().strip().split('\n')]
)
