from distutils.core import setup
from pip.req import parse_requirements

install_reqs = [str(r.req) for r in parse_requirements('requirements.txt', session='hack')]

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
    install_requires=install_reqs
)
