import logging

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('logger')


def logger(func):
    def wrapper(*args, **kwargs):
        logging_string = 'm={}'

        if args and len(args) > 1:
            complete_args = ''
            for index, arg in enumerate(args):
                if index == 0 or 'self' in str(arg):
                    continue

                arg_name = func.__code__.co_varnames[index]
                complete_args += '{}={}, '.format(arg_name, arg)

            if len(complete_args) > 0:
                complete_args = complete_args[:-2]

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
