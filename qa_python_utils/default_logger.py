# TODO allow handler to be chosen (file or stream)
# TODO fix nested decorators losing class name
# TODO overwrite message methods (info, warning...) to have a standard message

import logging


class QuintoAndarLogger(logging.Logger):
    """This class implements a default logger for QuintoAndar and has some
    additions to how logging.Logger works:
        - format can be set when instantiating an object.
        - an object of this class can be used as a decorator to show logs a
            function call.
    """

    def __init__(
            self,
            name='root',
            level=logging.INFO,
            fmt='%(levelname)s:%(name)s:%(asctime)s:%(message)s',
            datefmt=None):
        super(QuintoAndarLogger, self).__init__(name, level)

        # create a handler for the logger object
        stream_handler = logging.StreamHandler()

        # create a formatter
        formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

        # set format to the handler
        stream_handler.setFormatter(formatter)

        # set handler to be used
        self.addHandler(stream_handler)

    def __call__(self, func=None, exclude=None):
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

        def _wrapper(*args, **kwargs):
            logging_string = 'm={}'

            # gets the method name
            method_name = 'unnamed'
            if hasattr(func, '__name__'):
                method_name = func.__name__

            # gets the class name
            class_name = 'unnamed'
            if args and hasattr(args[0], '__class__'):
                class_name = args[0].__class__.__name__
            elif hasattr(func, '__class__'):
                class_name = func.__class__.__name__

            if args is not None and len(args) > 0:
                complete_args = ''
                has_multiple_params = len(func.__code__.co_varnames) < len(args)
                for index, arg in enumerate(args):
                    if index >= len(func.__code__.co_varnames):
                        complete_args += (
                            ('{})' if index == len(args) - 1 else '{}, ')
                                .format(arg)
                        )
                        continue

                    arg_name = func.__code__.co_varnames[index]
                    if 'self' in arg_name or (exclude and arg_name in exclude):
                        continue

                    complete_args += (
                        ('*{}=({}, '
                         if len(args) > func.__code__.co_argcount
                            and index >= func.__code__.co_argcount
                         else '{}={}, ').format(arg_name, arg)
                    )

                if len(complete_args) > 0 and not has_multiple_params:
                    complete_args = complete_args[:-2]
                    complete_args += (
                        ')' if len(args) > func.__code__.co_argcount else '')

                if len(complete_args) > 0:
                    logging_string += ', {}'.format(complete_args)

            if (kwargs is not None and
                    len(kwargs) > 0 and
                    exclude not in func.__code__.co_varnames):
                logging_string += ', kwargs={}'

            if ((args is None or len(args) <= 1)
                    and (kwargs is None or len(kwargs) <= 1)):
                logging_string += ', msg=init'

            # show log
            self.info(
                logging_string.format(
                    '{}.{}'.format(class_name, method_name),
                    kwargs)
            )
            try:
                return func(*args, **kwargs)
            except Exception, e:
                self.exception(
                    'm={}, msg={}'.format(method_name, e))
                raise e

        def _partial_wrapper(func):
            return self(func, exclude)

        if func is None:
            return _partial_wrapper
        else:
            return _wrapper
