from functools import wraps

import ziplime.api
from ziplime.utils.algo_instance import get_algo_instance, set_algo_instance


class ZiplineAPI(object):
    """
    Context manager for making an algorithm instance available to ziplime API
    functions within a scoped block.
    """

    def __init__(self, algo_instance):
        self.algo_instance = algo_instance

    def __enter__(self):
        """
        Set the given algo instance, storing any previously-existing instance.
        """
        self.old_algo_instance = get_algo_instance()
        set_algo_instance(self.algo_instance)

    def __exit__(self, _type, _value, _tb):
        """
        Restore the algo instance stored in __enter__.
        """
        set_algo_instance(self.old_algo_instance)


def api_method(f):
    # Decorator that adds the decorated class method as a callable
    # function (wrapped) to ziplime.api
    @wraps(f)
    def wrapped(*args, **kwargs):
        # Get the instance and call the method
        algo_instance = get_algo_instance()
        if algo_instance is None:
            raise RuntimeError(
                'ziplime api method %s must be called during a simulation.'
                % f.__name__
            )
        return getattr(algo_instance, f.__name__)(*args, **kwargs)
    # Add functor to ziplime.api
    setattr(ziplime.api, f.__name__, wrapped)
    ziplime.api.__all__.append(f.__name__)
    f.is_api_method = True
    return f


def require_not_initialized(exception):
    """
    Decorator for API methods that should only be called during or before
    TradingAlgorithm.initialize.  `exception` will be raised if the method is
    called after initialize.

    Usage
    -----
    @require_not_initialized(SomeException("Don't do that!"))
    def method(self):
        # Do stuff that should only be allowed during initialize.
    """
    def decorator(method):
        @wraps(method)
        def wrapped_method(self, *args, **kwargs):
            if self.initialized:
                raise exception
            return method(self, *args, **kwargs)
        return wrapped_method
    return decorator


def require_initialized(exception):
    """
    Decorator for API methods that should only be called after
    TradingAlgorithm.initialize.  `exception` will be raised if the method is
    called before initialize has completed.

    Usage
    -----
    @require_initialized(SomeException("Don't do that!"))
    def method(self):
        # Do stuff that should only be allowed after initialize.
    """
    def decorator(method):
        @wraps(method)
        def wrapped_method(self, *args, **kwargs):
            if not self.initialized:
                raise exception
            return method(self, *args, **kwargs)
        return wrapped_method
    return decorator


def disallowed_in_before_trading_start(exception):
    """
    Decorator for API methods that cannot be called from within
    TradingAlgorithm.before_trading_start.  `exception` will be raised if the
    method is called inside `before_trading_start`.

    Usage
    -----
    @disallowed_in_before_trading_start(SomeException("Don't do that!"))
    def method(self):
        # Do stuff that is not allowed inside before_trading_start.
    """
    def decorator(method):
        @wraps(method)
        def wrapped_method(self, *args, **kwargs):
            if self._in_before_trading_start:
                raise exception
            return method(self, *args, **kwargs)
        return wrapped_method
    return decorator


# def allowed_only_in_before_trading_start(exception):
#     """
#     Decorator for API methods that can be called only from within
#     TradingAlgorithm.before_trading_start.  `exception` will be raised if the
#     method is called outside `before_trading_start`.
#
#     Usage
#     -----
#     @allowed_only_in_before_trading_start(SomeException("Don't do that!"))
#     def method(self):
#         # Do stuff that is only allowed inside before_trading_start.
#     """
#     def decorator(method):
#         @wraps(method)
#         def wrapped_method(self, *args, **kwargs):
#             if not self._in_before_trading_start:
#                 raise exception
#             return method(self, *args, **kwargs)
#         return wrapped_method
#     return decorator
