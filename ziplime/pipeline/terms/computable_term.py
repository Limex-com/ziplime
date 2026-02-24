from abc import abstractmethod
from bisect import insort

from numpy import (
    record,
    ndarray,
)
from ziplime.errors import (
    InvalidOutputName,
    NonWindowSafeInput,
    NonPipelineInputs,
    TermInputsNotSpecified,
    TermOutputsEmpty,
    WindowLengthNotSpecified,
)
from ziplime.pipeline.terms.term import Term
from ziplime.pipeline.domain import Domain
from ziplime.pipeline.domain import infer_domain
from ziplime.pipeline.downsample_helpers import SUPPORTED_DOWNSAMPLE_FREQUENCIES
from ziplime.pipeline.terms.asset_exists import AssetExists
from ziplime.pipeline.terms.loadable_term import LoadableTerm
from ziplime.pipeline.terms.utils import _coerce_to_dtype
from ziplime.utils.numpy_utils import (
    bool_dtype,
    float64_dtype,
)
from ziplime.utils.sharedoc import (
    templated_docstring,
    PIPELINE_ALIAS_NAME_DOC,
    PIPELINE_DOWNSAMPLING_FREQUENCY_DOC,
)


class ComputableTerm(Term):
    """
    A Term that should be computed from a tuple of inputs.

    This is the base class for :class:`ziplime.pipeline.Factor`,
    :class:`ziplime.pipeline.Filter`, and :class:`ziplime.pipeline.Classifier`.
    """

    inputs = None
    outputs = None
    window_length = None
    mask = None
    domain = None

    def __new__(
            cls,
            inputs=inputs,
            outputs=outputs,
            window_length=window_length,
            mask=mask,
            domain=domain,
            *args,
            **kwargs,
    ):

        if inputs is None:
            inputs = cls.inputs

        # Having inputs = None is an error, but we handle it later
        # in self._validate rather than here.
        if inputs is not None:
            # Allow users to specify lists as class-level defaults, but
            # normalize to a tuple so that inputs is hashable.
            inputs = tuple(inputs)

            # Make sure all our inputs are valid pipeline objects before trying
            # to infer a domain.
            non_terms = [t for t in inputs if not isinstance(t, Term)]
            if non_terms:
                raise NonPipelineInputs(cls.__name__, non_terms)

            if domain is None:
                domain = infer_domain(inputs)

        if outputs is None:
            outputs = cls.outputs
        if outputs is not None:
            outputs = tuple(outputs)

        if mask is None:
            mask = cls.mask
        if mask is None:
            mask = AssetExists()

        if window_length is None:
            window_length = cls.window_length

        return super(ComputableTerm, cls).__new__(
            cls,
            inputs=inputs,
            outputs=outputs,
            mask=mask,
            window_length=window_length,
            domain=domain,
            *args,
            **kwargs,
        )

    def _init(self, inputs, outputs, window_length, mask, *args, **kwargs):
        self.inputs = inputs
        self.outputs = outputs
        self.window_length = window_length
        self.mask = mask
        return super(ComputableTerm, self)._init(*args, **kwargs)

    @classmethod
    def _static_identity(cls, inputs, outputs, window_length, mask, *args, **kwargs):
        return (
            super(ComputableTerm, cls)._static_identity(*args, **kwargs),
            inputs,
            outputs,
            window_length,
            mask,
        )

    def _validate(self):
        super(ComputableTerm, self)._validate()

        # Check inputs.
        if self.inputs is None:
            raise TermInputsNotSpecified(termname=type(self).__name__)

        if not isinstance(self.domain, Domain):
            raise TypeError(
                "Expected {}.domain to be an instance of Domain, "
                "but got {}.".format(type(self).__name__, type(self.domain))
            )

        # Check outputs.
        if self.outputs is None:
            pass
        elif not self.outputs:
            raise TermOutputsEmpty(termname=type(self).__name__)
        else:
            # Raise an exception if there are any naming conflicts between the
            # term's output names and certain attributes.
            disallowed_names = [
                attr for attr in dir(ComputableTerm) if not attr.startswith("_")
            ]

            # The name 'compute' is an added special case that is disallowed.
            # Use insort to add it to the list in alphabetical order.
            insort(disallowed_names, "compute")

            for output in self.outputs:
                if output.startswith("_") or output in disallowed_names:
                    raise InvalidOutputName(
                        output_name=output,
                        termname=type(self).__name__,
                        disallowed_names=disallowed_names,
                    )

        if self.window_length is None:
            raise WindowLengthNotSpecified(termname=type(self).__name__)

        if self.mask is None:
            # This isn't user error, this is a bug in our code.
            raise AssertionError("{term} has no mask".format(term=self))

        if self.window_length > 1:
            for child in self.inputs:
                if not child.window_safe:
                    raise NonWindowSafeInput(parent=self, child=child)

    def _compute(self, inputs, dates, assets, mask):
        """
        Subclasses should implement this to perform actual computation.

        This is named ``_compute`` rather than just ``compute`` because
        ``compute`` is reserved for user-supplied functions in
        CustomFilter/CustomFactor/CustomClassifier.
        """
        raise NotImplementedError("_compute")

    # NOTE: This is a method rather than a property because ABCMeta tries to
    #       access all abstract attributes of its child classes to see if
    #       they've been implemented. These accesses happen during subclass
    #       creation, before the new subclass has been bound to a name in its
    #       defining scope. Filter, Factor, and Classifier each implement this
    #       method to return themselves, but if the method is invoked before
    #       class definition is finished (which happens if this is a property),
    #       they fail with a NameError.
    @classmethod
    @abstractmethod
    def _principal_computable_term_type(cls):
        """
        Return the "principal" type for a ComputableTerm.

        This returns either Filter, Factor, or Classifier, depending on the
        type of ``cls``. It is used to implement behaviors like ``downsample``
        and ``if_then_else`` that are implemented on all ComputableTerms, but
        that need to produce different output types depending on the type of
        the receiver.
        """
        raise NotImplementedError("_principal_computable_term_type")

    @property
    def windowed(self):
        """
        Whether or not this term represents a trailing window computation.

        If term.windowed is truthy, its compute_from_windows method will be
        called with instances of AdjustedArray as inputs.

        If term.windowed is falsey, its compute_from_baseline will be called
        with instances of np.ndarray as inputs.
        """
        return self.window_length is not None and self.window_length > 0

    @property
    def dependencies(self):
        """
        The number of extra rows needed for each of our inputs to compute this
        term.
        """
        extra_input_rows = max(0, self.window_length - 1)
        out = {}
        for term in self.inputs:
            out[term] = extra_input_rows
        out[self.mask] = 0
        return out

    def postprocess(self, data: ndarray):
        """
        Called with an result of ``self``, unravelled (i.e. 1-dimensional)
        after any user-defined screens have been applied.

        This is mostly useful for transforming the dtype of an output, e.g., to
        convert a LabelArray into a pandas Categorical.

        The default implementation is to just return data unchanged.
        """
        # starting with pandas 1.4, record arrays are no longer supported as DataFrame columns
        if isinstance(data[0], record):
            return [tuple(r) for r in data]
        return data

    def to_workspace_value(self, result, assets):
        """
        Called with a column of the result of a pipeline. This needs to put
        the data into a format that can be used in a workspace to continue
        doing computations.

        Parameters
        ----------
        result : pd.Series
            A multiindexed series with (dates, assets) whose values are the
            results of running this pipeline term over the dates.
        assets : pd.Index
            All of the assets being requested. This allows us to correctly
            shape the workspace value.

        Returns
        -------
        workspace_value : array-like
            An array like value that the engine can consume.
        """
        return (
            result.unstack()
            .fillna(self.missing_value)
            .reindex(columns=assets, fill_value=self.missing_value)
            .values
        )

    @templated_docstring(frequency=PIPELINE_DOWNSAMPLING_FREQUENCY_DOC)
    def downsample(self, frequency):
        """
        Make a term that computes from ``self`` at lower-than-daily frequency.

        Parameters
        ----------
        {frequency}
        """
        from ..mixins import DownsampledMixin
        if frequency not in SUPPORTED_DOWNSAMPLE_FREQUENCIES:
            raise ValueError(
                "Invalid downsampling frequency: {frequency}.\n\n"
                "Valid downsampling frequencies are: {valid_frequencies}".format(
                    frequency=frequency,
                    valid_frequencies=", ".join(
                        sorted(SUPPORTED_DOWNSAMPLE_FREQUENCIES)
                    ),
                )
            )
        downsampled_type = type(self)._with_mixin(DownsampledMixin)
        return downsampled_type(term=self, frequency=frequency)

    @templated_docstring(name=PIPELINE_ALIAS_NAME_DOC)
    def alias(self, name):
        """
        Make a term from ``self`` that names the expression.

        Parameters
        ----------
        {name}

        Returns
        -------
        aliased : Aliased
            ``self`` with a name.

        Notes
        -----
        This is useful for giving a name to a numerical or boolean expression.
        """
        from ..mixins import AliasedMixin

        aliased_type = type(self)._with_mixin(AliasedMixin)
        return aliased_type(term=self, name=name)

    def isnull(self):
        """
        A Filter producing True for values where this Factor has missing data.

        Equivalent to self.isnan() when ``self.dtype`` is float64.
        Otherwise equivalent to ``self.eq(self.missing_value)``.

        Returns
        -------
        filter : ziplime.pipeline.Filter
        """
        if self.dtype == bool_dtype:
            raise TypeError("isnull() is not supported for Filters")

        from .filters import NullFilter

        if self.dtype == float64_dtype:
            # Using isnan is more efficient when possible because we can fold
            # the isnan computation with other NumExpr expressions.
            return self.isnan()
        else:
            return NullFilter(self)

    def notnull(self):
        """
        A Filter producing True for values where this Factor has complete data.

        Equivalent to ``~self.isnan()` when ``self.dtype`` is float64.
        Otherwise equivalent to ``(self != self.missing_value)``.

        Returns
        -------
        filter : ziplime.pipeline.Filter
        """
        if self.dtype == bool_dtype:
            raise TypeError("notnull() is not supported for Filters")

        from .filters import NotNullFilter

        return NotNullFilter(self)

    def fillna(self, fill_value):
        """
        Create a new term that fills missing values of this term's output with
        ``fill_value``.

        Parameters
        ----------
        fill_value : ziplime.pipeline.ComputableTerm, or object.
            Object to use as replacement for missing values.

            If a ComputableTerm (e.g. a Factor) is passed, that term's results
            will be used as fill values.

            If a scalar (e.g. a number) is passed, the scalar will be used as a
            fill value.

        Examples
        --------

        **Filling with a Scalar:**

        Let ``f`` be a Factor which would produce the following output::

                         AAPL   MSFT    MCD     BK
            2017-03-13    1.0    NaN    3.0    4.0
            2017-03-14    1.5    2.5    NaN    NaN

        Then ``f.fillna(0)`` produces the following output::

                         AAPL   MSFT    MCD     BK
            2017-03-13    1.0    0.0    3.0    4.0
            2017-03-14    1.5    2.5    0.0    0.0

        **Filling with a Term:**

        Let ``f`` be as above, and let ``g`` be another Factor which would
        produce the following output::

                         AAPL   MSFT    MCD     BK
            2017-03-13   10.0   20.0   30.0   40.0
            2017-03-14   15.0   25.0   35.0   45.0

        Then, ``f.fillna(g)`` produces the following output::

                         AAPL   MSFT    MCD     BK
            2017-03-13    1.0   20.0    3.0    4.0
            2017-03-14    1.5    2.5   35.0   45.0

        Returns
        -------
        filled : ziplime.pipeline.ComputableTerm
            A term computing the same results as ``self``, but with missing
            values filled in using values from ``fill_value``.
        """
        if self.dtype == bool_dtype:
            raise TypeError("fillna() is not supported for Filters")

        if isinstance(fill_value, LoadableTerm):
            raise TypeError(
                "Can't use expression {} as a fill value. Did you mean to "
                "append '.latest?'".format(fill_value)
            )
        elif isinstance(fill_value, ComputableTerm):
            if_false = fill_value
        else:
            # Assume we got a scalar value. Make sure it's compatible with our
            # dtype.
            try:
                fill_value = _coerce_to_dtype(fill_value, self.dtype)
            except TypeError as exc:
                raise TypeError(
                    "Fill value {value!r} is not a valid choice "
                    "for term {termname} with dtype {dtype}.\n\n"
                    "Coercion attempt failed with: {error}".format(
                        termname=type(self).__name__,
                        value=fill_value,
                        dtype=self.dtype,
                        error=exc,
                    )
                ) from exc

            if_false = self._constant_type(
                const=fill_value,
                dtype=self.dtype,
                missing_value=self.missing_value,
            )

        return self.notnull().if_else(if_true=self, if_false=if_false)

    # @classlazyval
    @property
    def _constant_type(cls):
        from ..mixins import ConstantMixin

        return cls._with_mixin(ConstantMixin)

    # @classlazyval
    @property
    def _if_else_type(cls):
        from ..mixins import IfElseMixin

        return cls._with_mixin(IfElseMixin)

    def __repr__(self):
        return ("{type}([{inputs}], {window_length})").format(
            type=type(self).__name__,
            inputs=", ".join(i.recursive_repr() for i in self.inputs),
            window_length=self.window_length,
        )

    def recursive_repr(self):
        return type(self).__name__ + "(...)"

    @classmethod
    def _with_mixin(cls, mixin_type):
        return mixin_type.universal_mixin_specialization(
            cls._principal_computable_term_type(),
        )
