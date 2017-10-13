import logging

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('logger')


def logger(func):
    def wrapper(*args, **kwargs):
        logging_string = 'm={}'

        if args and len(args) > 1:
            complete_args = ''
            has_multiple_params = len(func.__code__.co_varnames) < len(args)
            for index, arg in enumerate(args):
                if index >= len(func.__code__.co_varnames):
                    complete_args += ('{})' if index == len(args) - 1 else '{}, ').format(arg)
                    continue

                arg_name = func.__code__.co_varnames[index]
                if 'self' in arg_name:
                    continue

                complete_args += ('*{}=({}, ' if len(args) > func.__code__.co_argcount
                                                 and index >= func.__code__.co_argcount else '{}={}, ').format(arg_name,
                                                                                                               arg)

            if len(complete_args) > 0 and not has_multiple_params:
                complete_args = complete_args[:-2]
                if len(args) > func.__code__.co_argcount:
                    complete_args += ')'

            logging_string += ', {}'.format(complete_args)

        if kwargs and len(kwargs) > 0:
            logging_string += ', kwargs={}'

        if (not args or len(args) <= 1) and (not kwargs and len(kwargs) <= 0):
            logging_string += ', msg=init'

        _logger.info(logging_string.format(func.__name__, kwargs))
        try:
            return func(*args, **kwargs)
        except:
            _logger.exception('m={}, msg=exception'.format(func.__name__))
            raise

    return wrapper
