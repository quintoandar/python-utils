# QuintoAndar Python Utils
A simple set of Python utils used at QuintoAndar.com

## Prerequisites
* python3
* python3-pip

## Installing Python dependencies
```
pip3 install -r requirements.txt
```

## Installing npm dependencies
```
npm install
```

## Flake8
We use Flake8 to enforce PEP 8 style guide code. It's triggered right before you try to commit something.
So, if you want to check if your code respect most of PEP 8 guides, just run `flake8` in command line.


## Google utils

### Creating google service account
Follow instructions at `https://developers.google.com/api-client-library/python/auth/service-accounts`

Save private key JSON in $QA_PYTHON_UTILS_CREDENTIALS_JSON

After that, don't forget to share your Drive folder with the client email generated in the json (this info can be found in `client_email` field).


## New modules
Whenever we create a new module (i.e. a new package under the `qa_python_utils` directory), we have to add it in the package section of the `setup.py` file
