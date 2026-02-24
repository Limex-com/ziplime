"""
Base class for Filters, Factors and Classifiers
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from weakref import WeakValueDictionary


from ziplime.errors import NonSliceableTerm

from ..domain import GENERIC
from ziplime.assets.entities.asset import Asset
from .utils import validate_dtype


class Term(ABC):
    """
    Base class for objects that can appear in the compute graph of a
    :class:`ziplime.pipeline.Pipeline`.

    Notes
    -----
    Most Pipeline API users only interact with :class:`Term` via subclasses:

    - :class:`~ziplime.pipeline.data.BoundColumn`
    - :class:`~ziplime.pipeline.Factor`
    - :class:`~ziplime.pipeline.Filter`
    - :class:`~ziplime.pipeline.Classifier`

    Instances of :class:`Term` are **memoized**. If you call a Term's
    constructor with the same arguments twice, the same object will be returned
    from both calls:

    **Example:**

    >>> from ziplime.pipeline.data import EquityPricing
    >>> from ziplime.pipeline.factors import SimpleMovingAverage
    >>> x = SimpleMovingAverage(inputs=[EquityPricing.close], window_length=5)
    >>> y = SimpleMovingAverage(inputs=[EquityPricing.close], window_length=5)
    >>> x is y
    True

    .. warning::

       Memoization of terms means that it's generally unsafe to modify
       attributes of a term after construction.
    """

    # These are NotSpecified because a subclass is required to provide them.
    dtype = None
    missing_value = None

    # Subclasses aren't required to provide `params`.  The default behavior is
    # no params.
    params = ()

    # All terms are generic by default.
    domain = GENERIC

    # Determines if a term is safe to be used as a windowed input.
    window_safe = False

    # The dimensions of the term's output (1D or 2D).
    ndim = 2

    _term_cache = WeakValueDictionary()

    def __new__(
        cls,
        domain=None,
        dtype=None,
        missing_value=None,
        window_safe=None,
        ndim=None,
        # params is explicitly not allowed to be passed to an instance.
        *args,
        **kwargs,
    ):
        """
        Memoized constructor for Terms.

        Caching previously-constructed Terms is useful because it allows us to
        only compute equivalent sub-expressions once when traversing a Pipeline
        dependency graph.

        Caching previously-constructed Terms is **sane** because terms and
        their inputs are both conceptually immutable.
        """
        # Subclasses can override these class-level attributes to provide
        # different default values for instances.
        if domain is None:
            domain = cls.domain
        if dtype is None:
            dtype = cls.dtype
        if missing_value is None:
            missing_value = cls.missing_value
        if ndim is None:
            ndim = cls.ndim
        if window_safe is None:
            window_safe = cls.window_safe

        dtype, missing_value = validate_dtype(
            cls.__name__,
            dtype,
            missing_value,
        )
        params = cls._pop_params(kwargs)

        identity = cls._static_identity(
            domain=domain,
            dtype=dtype,
            missing_value=missing_value,
            window_safe=window_safe,
            ndim=ndim,
            params=params,
            *args,
            **kwargs,
        )

        try:
            return cls._term_cache[identity]
        except KeyError:
            new_instance = cls._term_cache[identity] = (
                super(Term, cls)
                .__new__(cls)
                ._init(
                    domain=domain,
                    dtype=dtype,
                    missing_value=missing_value,
                    window_safe=window_safe,
                    ndim=ndim,
                    params=params,
                    *args,
                    **kwargs,
                )
            )
            return new_instance

    @classmethod
    def _pop_params(cls, kwargs):
        """
        Pop entries from the `kwargs` passed to cls.__new__ based on the values
        in `cls.params`.

        Parameters
        ----------
        kwargs : dict
            The kwargs passed to cls.__new__.

        Returns
        -------
        params : list[(str, object)]
            A list of string, value pairs containing the entries in cls.params.

        Raises
        ------
        TypeError
            Raised if any parameter values are not passed or not hashable.
        """
        params = cls.params
        if not isinstance(params, Mapping):
            params = {k: None for k in params}
        param_values = []
        for key, default_value in params.items():
            try:
                value = kwargs.pop(key, default_value)
                if value is None:
                    raise KeyError(key)

                # Check here that the value is hashable so that we fail here
                # instead of trying to hash the param values tuple later.
                hash(value)
            except KeyError as exc:
                raise TypeError(
                    "{typename} expected a keyword parameter {name!r}.".format(
                        typename=cls.__name__, name=key
                    )
                ) from exc
            except TypeError as exc:
                # Value wasn't hashable.
                raise TypeError(
                    "{typename} expected a hashable value for parameter "
                    "{name!r}, but got {value!r} instead.".format(
                        typename=cls.__name__,
                        name=key,
                        value=value,
                    )
                ) from exc

            param_values.append((key, value))
        return tuple(param_values)

    def __init__(self, *args, **kwargs):
        """
        Noop constructor to play nicely with our caching __new__.  Subclasses
        should implement _init instead of this method.

        When a class' __new__ returns an instance of that class, Python will
        automatically call __init__ on the object, even if a new object wasn't
        actually constructed.  Because we memoize instances, we often return an
        object that was already initialized from __new__, in which case we
        don't want to call __init__ again.

        Subclasses that need to initialize new instances should override _init,
        which is guaranteed to be called only once.
        """
        pass

    def __getitem__(self, key: Asset):
        from ziplime.pipeline.terms.loadable_term import LoadableTerm
        if isinstance(self, LoadableTerm):
            raise NonSliceableTerm(term=self)

        from ..mixins import SliceMixin

        slice_type = type(self)._with_mixin(SliceMixin)
        return slice_type(self, key)

    @classmethod
    def _static_identity(cls, domain, dtype, missing_value, window_safe, ndim, params):
        """
        Return the identity of the Term that would be constructed from the
        given arguments.

        Identities that compare equal will cause us to return a cached instance
        rather than constructing a new one.  We do this primarily because it
        makes dependency resolution easier.

        This is a classmethod so that it can be called from Term.__new__ to
        determine whether to produce a new instance.
        """
        return (cls, domain, dtype, missing_value, window_safe, ndim, params)

    def _init(self, domain, dtype, missing_value, window_safe, ndim, params):
        """
        Parameters
        ----------
        domain : ziplime.pipeline.domain.Domain
            The domain of this term.
        dtype : np.dtype
            Dtype of this term's output.
        missing_value : object
            Missing value for this term.
        ndim : 1 or 2
            The dimensionality of this term.
        params : tuple[(str, hashable)]
            Tuple of key/value pairs of additional parameters.
        """
        self.domain = domain
        self.dtype = dtype
        self.missing_value = missing_value
        self.window_safe = window_safe
        self.ndim = ndim

        for name, _ in params:
            if hasattr(self, name):
                raise TypeError(
                    "Parameter {name!r} conflicts with already-present"
                    " attribute with value {value!r}.".format(
                        name=name,
                        value=getattr(self, name),
                    )
                )
            # TODO: Consider setting these values as attributes and replacing
            # the boilerplate in NumericalExpression, Rank, and
            # PercentileFilter.

        self.params = dict(params)

        # Make sure that subclasses call super() in their _validate() methods
        # by setting this flag.  The base class implementation of _validate
        # should set this flag to True.
        self._subclass_called_super_validate = False
        self._validate()
        assert self._subclass_called_super_validate, (
            "Term._validate() was not called.\n"
            "This probably means that you overrode _validate"
            " without calling super()."
        )
        del self._subclass_called_super_validate

        return self

    def _validate(self):
        """
        Assert that this term is well-formed.  This should be called exactly
        once, at the end of Term._init().
        """
        # mark that we got here to enforce that subclasses overriding _validate
        # call super().
        self._subclass_called_super_validate = True

    def compute_extra_rows(self, all_dates, start_date, end_date, min_extra_rows):
        """
        Calculate the number of extra rows needed to compute ``self``.

        Must return at least ``min_extra_rows``, and the default implementation
        is to just return ``min_extra_rows``.  This is overridden by
        downsampled terms to ensure that the first date computed is a
        recomputation date.

        Parameters
        ----------
        all_dates : pd.DatetimeIndex
            The trading sessions against which ``self`` will be computed.
        start_date : pd.Timestamp
            The first date for which final output is requested.
        end_date : pd.Timestamp
            The last date for which final output is requested.
        min_extra_rows : int
            The minimum number of extra rows required of ``self``, as
            determined by other terms that depend on ``self``.

        Returns
        -------
        extra_rows : int
            The number of extra rows to compute.  Must be at least
            ``min_extra_rows``.
        """
        return min_extra_rows

    @property
    @abstractmethod
    def inputs(self):
        """
        A tuple of other Terms needed as inputs for ``self``.
        """
        raise NotImplementedError("inputs")

    @property
    @abstractmethod
    def windowed(self):
        """
        Boolean indicating whether this term is a trailing-window computation.
        """
        raise NotImplementedError("windowed")

    @property
    @abstractmethod
    def mask(self):
        """
        A :class:`~ziplime.pipeline.Filter` representing asset/date pairs to
        while computing this Term. True means include; False means exclude.
        """
        raise NotImplementedError("mask")

    @property
    @abstractmethod
    def dependencies(self):
        """
        A dictionary mapping terms that must be computed before `self` to the
        number of extra rows needed for those terms.
        """
        raise NotImplementedError("dependencies")

    def graph_repr(self):
        """A short repr to use when rendering GraphViz graphs."""
        # Default graph_repr is just the name of the type.
        return type(self).__name__

    def recursive_repr(self):
        """A short repr to use when recursively rendering terms with inputs."""
        # Default recursive_repr is just the name of the type.
        return type(self).__name__



