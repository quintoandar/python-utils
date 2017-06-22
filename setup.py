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
    install_requires=[
        'google-api-python-client==1.6.2',
        'oauth2client==4.1.0'
        'httplib2shim==0.0.1']

)
