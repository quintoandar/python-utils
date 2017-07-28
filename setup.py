from distutils.core import setup

setup(
    name='qa-python-utils',
    version='0.0.3',
    packages=['qa_python_utils',
              'qa_python_utils.google',
              'qa_python_utils.aws'],
    url='https://github.com/quintoandar/python-utils',
    license='',
    author='Quinto Andar',
    author_email='enishime@quintoandar.com.br',
    description='Package for python utils',
    install_requires=[open('requirements.txt').read().strip().split('\n')]
)
