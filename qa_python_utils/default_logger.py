import inspect
import logging

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('logger')


def logger(func=None, exclude=None):
    """
    Usage:
        @logger
        def some_function([cls, self, None], x, y):
            pass

        @logger(exclude='x')
        def some_function(x, y):
            pass

        @logger(exclude=['x', 'y']):
            pass
    :param func: function to be logged
    :param exclude: param names to be excluded in the logging string
    :return: complete logging string
    """

    try:
        __logger = logging.getLogger(inspect.stack()[-2][3])
    except IndexError:
        __logger = _logger

    if func is None:
        def partial_wrapper(func):
            return logger(func, exclude)

        return partial_wrapper
    else:
        def _wrapper(*args, **kwargs):
            logging_string = 'm={}'

            if args is not None and len(args) > 0:
                complete_args = ''
                has_multiple_params = len(func.__code__.co_varnames) < len(args)
                for index, arg in enumerate(args):
                    if index >= len(func.__code__.co_varnames):
                        complete_args += ('{})' if index == len(args) - 1 else '{}, ').format(arg)
                        continue

                    arg_name = func.__code__.co_varnames[index]
                    if 'self' in arg_name or (exclude and arg_name in exclude):
                        continue

                    complete_args += ('*{}=({}, ' if len(args) > func.__code__.co_argcount
                                                     and index >= func.__code__.co_argcount else '{}={}, ').format(
                        arg_name,
                        arg)

                if len(complete_args) > 0 and not has_multiple_params:
                    complete_args = complete_args[:-2]
                    complete_args += ')' if len(args) > func.__code__.co_argcount else ''

                if len(complete_args) > 0:
                    logging_string += ', {}'.format(complete_args)

            if kwargs is not None and len(kwargs) > 0 and exclude not in func.__code__.co_varnames:
                logging_string += ', kwargs={}'

            if (args is None or len(args) <= 1) and (kwargs is None and len(kwargs) <= 1):
                logging_string += ', msg=init'

            __logger.info(logging_string.format(func.__name__, kwargs))
            try:
                return func(*args, **kwargs)
            except:
                __logger.exception('m={}, msg=exception'.format(func.__name__))
                raise

        return _wrapper


if __name__ == '__main__':
    class LoggerTester:
        def __init__(self):
            pass

        class SubTester:
            def __init__(self):
                pass

            @logger
            def sub_test(self, x):
                print x

        sub_tester = SubTester()

        @logger(exclude='x')
        def test(self, x, y):
            print x, y


    def ultra_test(x):
        tester.sub_tester.sub_test(x)


    def ultra_mega_test(x):
        ultra_test(x)


    tester = LoggerTester()
    tester.sub_tester.sub_test('test1')
    tester.test('test2', 'test3')

    ultra_test('test4')

    ultra_mega_test('test5')
