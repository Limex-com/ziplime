"""
classifier.py
"""

from functools import partial
from numbers import Number
import operator
import re

import pandas as pd

from ziplime.lib.labelarray import LabelArray
from ziplime.pipeline.dtypes import (
    CLASSIFIER_DTYPES,
)
from ziplime.pipeline.terms.computable_term import ComputableTerm
from ziplime.utils.compat import unicode
from ziplime.utils.numpy_utils import (
    categorical_dtype,
    int64_dtype,
    vectorized_is_element,
)

from ziplime.pipeline.terms.filters import ArrayPredicate, NumExprFilter
from ziplime.pipeline.mixins import (
    RestrictedDTypeMixin,
)


class Classifier(RestrictedDTypeMixin, ComputableTerm):
    """
    A Pipeline expression computing a categorical output.

    Classifiers are most commonly useful for describing grouping keys for
    complex transformations on Factor outputs. For example, Factor.demean() and
    Factor.zscore() can be passed a Classifier in their ``groupby`` argument,
    indicating that means/standard deviations should be computed on assets for
    which the classifier produced the same label.
    """

    # Used by RestrictedDTypeMixin
    ALLOWED_DTYPES = CLASSIFIER_DTYPES
    categories = None

    # We explicitly don't support classifier to classifier comparisons, since
    # the stored values likely don't mean the same thing. This may be relaxed
    # in the future, but for now we're starting conservatively.
    def eq(self, other):
        """
        Construct a Filter returning True for asset/date pairs where the output
        of ``self`` matches ``other``.
        """
        # We treat this as an error because missing_values have NaN semantics,
        # which means this would return an array of all False, which is almost
        # certainly not what the user wants.
        if other == self.missing_value:
            raise ValueError(
                "Comparison against self.missing_value ({value!r}) in"
                " {typename}.eq().\n"
                "Missing values have NaN semantics, so the "
                "requested comparison would always produce False.\n"
                "Use the isnull() method to check for missing values.".format(
                    value=other,
                    typename=(type(self).__name__),
                )
            )

        if isinstance(other, Number) != (self.dtype == int64_dtype):
            raise InvalidClassifierComparison(self, other)

        if isinstance(other, Number):
            return NumExprFilter.create(
                "x_0 == {other}".format(other=int(other)),
                binds=(self,),
            )
        else:
            return ArrayPredicate(
                term=self,
                op=operator.eq,
                opargs=(other,),
            )

    def __ne__(self, other):
        """
        Construct a Filter returning True for asset/date pairs where the output
        of ``self`` matches ``other.
        """
        if isinstance(other, Number) != (self.dtype == int64_dtype):
            raise InvalidClassifierComparison(self, other)

        if isinstance(other, Number):
            return NumExprFilter.create(
                "((x_0 != {other}) & (x_0 != {missing}))".format(
                    other=int(other),
                    missing=self.missing_value,
                ),
                binds=(self,),
            )
        else:
            # Numexpr doesn't know how to use LabelArrays.
            return ArrayPredicate(term=self, op=operator.ne, opargs=(other,))

    def bad_compare(opname, other):
        raise TypeError("cannot compare classifiers with %s" % opname)

    __gt__ = partial(bad_compare, ">")
    __ge__ = partial(bad_compare, ">=")
    __le__ = partial(bad_compare, "<=")
    __lt__ = partial(bad_compare, "<")

    del bad_compare

    #@string_classifiers_only
    def startswith(self, prefix: bytes | unicode):
        """
        Construct a Filter matching values starting with ``prefix``.

        Parameters
        ----------
        prefix : str
            String prefix against which to compare values produced by ``self``.

        Returns
        -------
        matches : Filter
            Filter returning True for all sid/date pairs for which ``self``
            produces a string starting with ``prefix``.
        """
        return ArrayPredicate(
            term=self,
            op=LabelArray.startswith,
            opargs=(prefix,),
        )

    #@string_classifiers_only
    def endswith(self, suffix: bytes | unicode):
        """
        Construct a Filter matching values ending with ``suffix``.

        Parameters
        ----------
        suffix : str
            String suffix against which to compare values produced by ``self``.

        Returns
        -------
        matches : Filter
            Filter returning True for all sid/date pairs for which ``self``
            produces a string ending with ``prefix``.
        """
        return ArrayPredicate(
            term=self,
            op=LabelArray.endswith,
            opargs=(suffix,),
        )

    #@string_classifiers_only
    def has_substring(self, substring: bytes | unicode):
        """
        Construct a Filter matching values containing ``substring``.

        Parameters
        ----------
        substring : str
            Sub-string against which to compare values produced by ``self``.

        Returns
        -------
        matches : Filter
            Filter returning True for all sid/date pairs for which ``self``
            produces a string containing ``substring``.
        """
        return ArrayPredicate(
            term=self,
            op=LabelArray.has_substring,
            opargs=(substring,),
        )

    #@string_classifiers_only
    def matches(self, pattern: bytes | unicode | type(re.compile(""))):
        """
        Construct a Filter that checks regex matches against ``pattern``.

        Parameters
        ----------
        pattern : str
            Regex pattern against which to compare values produced by ``self``.

        Returns
        -------
        matches : Filter
            Filter returning True for all sid/date pairs for which ``self``
            produces a string matched by ``pattern``.

        See Also
        --------
        :mod:`Python Regular Expressions <re>`
        """
        return ArrayPredicate(
            term=self,
            op=LabelArray.matches,
            opargs=(pattern,),
        )

    # TODO: Support relabeling for integer dtypes.
    #@string_classifiers_only
    def relabel(self, relabeler):
        """
        Convert ``self`` into a new classifier by mapping a function over each
        element produced by ``self``.

        Parameters
        ----------
        relabeler : function[str -> str or None]
            A function to apply to each unique value produced by ``self``.

        Returns
        -------
        relabeled : Classifier
            A classifier produced by applying ``relabeler`` to each unique
            value produced by ``self``.
        """
        return Relabel(term=self, relabeler=relabeler)

    def element_of(self, choices):
        """
        Construct a Filter indicating whether values are in ``choices``.

        Parameters
        ----------
        choices : iterable[str or int]
            An iterable of choices.

        Returns
        -------
        matches : Filter
            Filter returning True for all sid/date pairs for which ``self``
            produces an entry in ``choices``.
        """
        try:
            choices = frozenset(choices)
        except Exception as exc:
            raise TypeError(
                "Expected `choices` to be an iterable of hashable values,"
                " but got {} instead.\n"
                "This caused the following error: {!r}.".format(choices, exc)
            ) from exc

        if self.missing_value in choices:
            raise ValueError(
                "Found self.missing_value ({mv!r}) in choices supplied to"
                " {typename}.{meth_name}().\n"
                "Missing values have NaN semantics, so the"
                " requested comparison would always produce False.\n"
                "Use the isnull() method to check for missing values.\n"
                "Received choices were {choices}.".format(
                    mv=self.missing_value,
                    typename=(type(self).__name__),
                    choices=sorted(choices),
                    meth_name=self.element_of.__name__,
                )
            )

        def only_contains(type_, values):
            return all(isinstance(v, type_) for v in values)

        if self.dtype == int64_dtype:
            if only_contains(int, choices):
                return ArrayPredicate(
                    term=self,
                    op=vectorized_is_element,
                    opargs=(choices,),
                )
            else:
                raise TypeError(
                    "Found non-int in choices for {typename}.element_of.\n"
                    "Supplied choices were {choices}.".format(
                        typename=type(self).__name__,
                        choices=choices,
                    )
                )
        elif self.dtype == categorical_dtype:
            if only_contains((bytes, unicode), choices):
                return ArrayPredicate(
                    term=self,
                    op=LabelArray.element_of,
                    opargs=(choices,),
                )
            else:
                raise TypeError(
                    "Found non-string in choices for {typename}.element_of.\n"
                    "Supplied choices were {choices}.".format(
                        typename=type(self).__name__,
                        choices=choices,
                    )
                )
        raise AssertionError(f"Unknown dtype in Classifier.element_of {self.dtype}.")

    def postprocess(self, data):
        if self.dtype == int64_dtype:
            return data
        if not isinstance(data, LabelArray):
            raise AssertionError("Expected a LabelArray, got %s." % type(data))
        return data.as_categorical()

    def to_workspace_value(self, result, assets):
        """
        Called with the result of a pipeline. This needs to return an object
        which can be put into the workspace to continue doing computations.

        This is the inverse of :func:`~ziplime.pipeline.term.Term.postprocess`.
        """
        if self.dtype == int64_dtype:
            return super(Classifier, self).to_workspace_value(result, assets)

        assert isinstance(
            result.values, pd.Categorical
        ), "Expected a Categorical, got %r." % type(result.values)
        with_missing = pd.Series(
            data=pd.Categorical(
                result.values,
                result.values.categories.union([self.missing_value]),
            ),
            index=result.index,
        )
        return LabelArray(
            super(Classifier, self).to_workspace_value(
                with_missing,
                assets,
            ),
            self.missing_value,
        )

    @classmethod
    def _principal_computable_term_type(cls):
        return Classifier

    def _to_integral(self, output_array):
        """
        Convert an array produced by this classifier into an array of integer
        labels and a missing value label.
        """
        if self.dtype == int64_dtype:
            group_labels = output_array
            null_label = self.missing_value
        elif self.dtype == categorical_dtype:
            # Coerce LabelArray into an isomorphic array of ints.  This is
            # necessary because np.where doesn't know about LabelArrays or the
            # void dtype.
            group_labels = output_array.as_int_array()
            null_label = output_array.missing_value_code
        else:
            raise AssertionError("Unexpected Classifier dtype: %s." % self.dtype)
        return group_labels, null_label

    def peer_count(self, mask=None):
        """
        Construct a factor that gives the number of occurrences of
        each distinct category in a classifier.

        Parameters
        ----------
        mask : ziplime.pipeline.Filter, optional
            If passed, only count assets passing the filter.  Default behavior
            is to count all assets.

        Examples
        --------
        Let ``c`` be a Classifier which would produce the following output::

                         AAPL   MSFT    MCD     BK   AMZN     FB
            2015-05-05    'a'    'a'   None    'b'    'a'   None
            2015-05-06    'b'    'a'    'c'    'b'    'b'    'b'
            2015-05-07   None    'a'   'aa'   'aa'   'aa'   None
            2015-05-08    'c'    'c'    'c'    'c'    'c'    'c'

        Then ``c.peer_count()`` will count, for each row, the total number
        of assets in each classifier category produced by ``c``.  Missing
        data will be evaluated to NaN.

        ::

                         AAPL   MSFT    MCD     BK   AMZN     FB
            2015-05-05    3.0    3.0    NaN    1.0    3.0    NaN
            2015-05-06    4.0    1.0    1.0    4.0    4.0    4.0
            2015-05-07    NaN    1.0    3.0    3.0    3.0    NaN
            2015-05-08    6.0    6.0    6.0    6.0    6.0    6.0

        Returns
        -------
        factor : CustomFactor
            A CustomFactor that counts, for each asset, the total number
            of assets with the same classifier category label.
        """
        # Lazy import due to cyclic dependencies in factor.py, classifier.py
        from ..factors import PeerCount

        return PeerCount(inputs=[self], mask=mask)

