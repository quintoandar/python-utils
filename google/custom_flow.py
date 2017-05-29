import sys
import oauth2client.client as client
import oauth2client.clientsecrets as clientsecrets


def flow_from_clientsecrets(client_dict, scope, redirect_uri=None,
                            message=None, cache=None, login_hint=None,
                            device_uri=None, pkce=None, code_verifier=None):
    '''
    Reimplementation of google Oauth2client, to use client secret
    from a string, instead of a file
    '''
    try:
        client_type, client_info = clientsecrets.loads(client_dict)
        if client_type in (clientsecrets.TYPE_WEB,
                           clientsecrets.TYPE_INSTALLED):
            constructor_kwargs = {
                'redirect_uri': redirect_uri,
                'auth_uri': client_info['auth_uri'],
                'token_uri': client_info['token_uri'],
                'login_hint': login_hint,
            }
            revoke_uri = client_info.get('revoke_uri')
            optional = ('revoke_uri', 'device_uri', 'pkce', 'code_verifier')
            for param in optional:  # pragma: no cover
                if locals()[param] is not None:
                    constructor_kwargs[param] = locals()[param]

            return client.OAuth2WebServerFlow(
                client_info['client_id'], client_info['client_secret'],
                scope, **constructor_kwargs)

    except clientsecrets.InvalidClientSecretsError as e:  # pragma: no cover
        if message is not None:
            if e.args:
                message = ('The client secrets were invalid: '
                           '\n{0}\n{1}'.format(e, message))
            sys.exit(message)
        else:
            raise
    else:  # pragma: no cover
        raise client.UnknownClientSecretsFlowError(
            'This OAuth 2.0 flow is unsupported: {0!r}'.format(client_type))
