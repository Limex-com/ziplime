from textwrap import dedent

from numpy import inf, nan
from scipy.stats import rankdata

from ziplime.pipeline.terms.computable_term import ComputableTerm
from ziplime.pipeline.terms.factors.utils.grouped_row_transform_utils import demean, zscore, winsorize
from ziplime.pipeline.terms.factors.utils.operators import binary_operator, reflected_binary_operator, unary_operator, \
    function_application
from ziplime.pipeline.terms.factors.utils.summary_funcs import summary_method, summary_funcs
from ziplime.pipeline.terms.term import Term
from ziplime.errors import BadPercentileBounds
from ziplime.lib.rank import rankdata_1d_descending
from ziplime.pipeline.terms.classifiers import Classifier, Quantiles
from ziplime.pipeline.dtypes import FACTOR_DTYPES

from ziplime.pipeline.expression import (
    COMPARISONS,
    MATH_BINOPS,
    method_name_for_op,
    NUMEXPR_MATH_FUNCS,
    UNARY_OPS,
    unary_op_name,
)
from ziplime.pipeline.terms.filters import (
    Filter,
    PercentileFilter,
    MaximumFilter,
)
from ziplime.pipeline.mixins import RestrictedDTypeMixin

from ziplime.utils.numpy_utils import float64_dtype
from ziplime.utils.sharedoc import templated_docstring



CORRELATION_METHOD_NOTE = dedent(
    """\
    This method can only be called on expressions which are deemed safe for use
    as inputs to windowed :class:`~ziplime.pipeline.Factor` objects. Examples
    of such expressions include This includes
    :class:`~ziplime.pipeline.data.BoundColumn`
    :class:`~ziplime.pipeline.factors.Returns` and any factors created from
    :meth:`~ziplime.pipeline.Factor.rank` or
    :meth:`~ziplime.pipeline.Factor.zscore`.
    """
)


class Factor(RestrictedDTypeMixin, ComputableTerm):
    """
    Pipeline API expression producing a numerical or date-valued output.

    Factors are the most commonly-used Pipeline term, representing the result
    of any computation producing a numerical result.

    Factors can be combined, both with other Factors and with scalar values,
    via any of the builtin mathematical operators (``+``, ``-``, ``*``, etc).

    This makes it easy to write complex expressions that combine multiple
    Factors. For example, constructing a Factor that computes the average of
    two other Factors is simply::

        >>> f1 = SomeFactor(...)  # doctest: +SKIP
        >>> f2 = SomeOtherFactor(...)  # doctest: +SKIP
        >>> average = (f1 + f2) / 2.0  # doctest: +SKIP

    Factors can also be converted into :class:`ziplime.pipeline.Filter` objects
    via comparison operators: (``<``, ``<=``, ``!=``, ``eq``, ``>``, ``>=``).

    There are many natural operators defined on Factors besides the basic
    numerical operators. These include methods for identifying missing or
    extreme-valued outputs (:meth:`isnull`, :meth:`notnull`, :meth:`isnan`,
    :meth:`notnan`), methods for normalizing outputs (:meth:`rank`,
    :meth:`demean`, :meth:`zscore`), and methods for constructing Filters based
    on rank-order properties of results (:meth:`top`, :meth:`bottom`,
    :meth:`percentile_between`).
    """

    ALLOWED_DTYPES = FACTOR_DTYPES  # Used by RestrictedDTypeMixin

    # Dynamically add functions for creating NumExprFactor/NumExprFilter
    # instances.
    clsdict = locals()
    clsdict.update(
        {
            method_name_for_op(op): binary_operator(op)
            # Don't override __eq__ because it breaks comparisons on tuples of
            # Factors.
            for op in MATH_BINOPS.union(COMPARISONS - {"=="})
        }
    )
    clsdict.update(
        {
            method_name_for_op(op, commute=True): reflected_binary_operator(op)
            for op in MATH_BINOPS
        }
    )
    clsdict.update({unary_op_name(op): unary_operator(op) for op in UNARY_OPS})

    clsdict.update(
        {funcname: function_application(funcname) for funcname in NUMEXPR_MATH_FUNCS}
    )

    __truediv__ = clsdict["__div__"]
    __rtruediv__ = clsdict["__rdiv__"]

    # Add summary functions.
    clsdict.update(
        {name: summary_method(name) for name in summary_funcs.names},
    )

    del clsdict  # don't pollute the class namespace with this.

    eq = binary_operator("==")

    #@float64_only
    def demean(self, mask: Filter | None = None,
               groupby: Classifier | None = None):
        """
        Construct a Factor that computes ``self`` and subtracts the mean from
        row of the result.

        If ``mask`` is supplied, ignore values where ``mask`` returns False
        when computing row means, and output NaN anywhere the mask is False.

        If ``groupby`` is supplied, compute by partitioning each row based on
        the values produced by ``groupby``, de-meaning the partitioned arrays,
        and stitching the sub-results back together.

        Parameters
        ----------
        mask : ziplime.pipeline.Filter, optional
            A Filter defining values to ignore when computing means.
        groupby : ziplime.pipeline.Classifier, optional
            A classifier defining partitions over which to compute means.

        Examples
        --------
        Let ``f`` be a Factor which would produce the following output::

                         AAPL   MSFT    MCD     BK
            2017-03-13    1.0    2.0    3.0    4.0
            2017-03-14    1.5    2.5    3.5    1.0
            2017-03-15    2.0    3.0    4.0    1.5
            2017-03-16    2.5    3.5    1.0    2.0

        Let ``c`` be a Classifier producing the following output::

                         AAPL   MSFT    MCD     BK
            2017-03-13      1      1      2      2
            2017-03-14      1      1      2      2
            2017-03-15      1      1      2      2
            2017-03-16      1      1      2      2

        Let ``m`` be a Filter producing the following output::

                         AAPL   MSFT    MCD     BK
            2017-03-13  False   True   True   True
            2017-03-14   True  False   True   True
            2017-03-15   True   True  False   True
            2017-03-16   True   True   True  False

        Then ``f.demean()`` will subtract the mean from each row produced by
        ``f``.

        ::

                         AAPL   MSFT    MCD     BK
            2017-03-13 -1.500 -0.500  0.500  1.500
            2017-03-14 -0.625  0.375  1.375 -1.125
            2017-03-15 -0.625  0.375  1.375 -1.125
            2017-03-16  0.250  1.250 -1.250 -0.250

        ``f.demean(mask=m)`` will subtract the mean from each row, but means
        will be calculated ignoring values on the diagonal, and NaNs will
        written to the diagonal in the output. Diagonal values are ignored
        because they are the locations where the mask ``m`` produced False.

        ::

                         AAPL   MSFT    MCD     BK
            2017-03-13    NaN -1.000  0.000  1.000
            2017-03-14 -0.500    NaN  1.500 -1.000
            2017-03-15 -0.166  0.833    NaN -0.666
            2017-03-16  0.166  1.166 -1.333    NaN

        ``f.demean(groupby=c)`` will subtract the group-mean of AAPL/MSFT and
        MCD/BK from their respective entries.  The AAPL/MSFT are grouped
        together because both assets always produce 1 in the output of the
        classifier ``c``.  Similarly, MCD/BK are grouped together because they
        always produce 2.

        ::

                         AAPL   MSFT    MCD     BK
            2017-03-13 -0.500  0.500 -0.500  0.500
            2017-03-14 -0.500  0.500  1.250 -1.250
            2017-03-15 -0.500  0.500  1.250 -1.250
            2017-03-16 -0.500  0.500 -0.500  0.500

        ``f.demean(mask=m, groupby=c)`` will also subtract the group-mean of
        AAPL/MSFT and MCD/BK, but means will be calculated ignoring values on
        the diagonal , and NaNs will be written to the diagonal in the output.

        ::

                         AAPL   MSFT    MCD     BK
            2017-03-13    NaN  0.000 -0.500  0.500
            2017-03-14  0.000    NaN  1.250 -1.250
            2017-03-15 -0.500  0.500    NaN  0.000
            2017-03-16 -0.500  0.500  0.000    NaN

        Notes
        -----
        Mean is sensitive to the magnitudes of outliers. When working with
        factor that can potentially produce large outliers, it is often useful
        to use the ``mask`` parameter to discard values at the extremes of the
        distribution::

            >>> base = MyFactor(...)  # doctest: +SKIP
            >>> normalized = base.demean(
            ...     mask=base.percentile_between(1, 99),
            ... )  # doctest: +SKIP

        ``demean()`` is only supported on Factors of dtype float64.

        See Also
        --------
        :meth:`pandas.DataFrame.groupby`
        """
        from ziplime.pipeline.terms.factors.grouped_row_transform import GroupedRowTransform

        return GroupedRowTransform(
            transform=demean,
            transform_args=(),
            factor=self,
            groupby=groupby,
            dtype=self.dtype,
            missing_value=self.missing_value,
            window_safe=self.window_safe,
            mask=mask,
        )

    #@float64_only
    def zscore(self, mask: Filter | None = None,
               groupby: Classifier | None = None):
        """
        Construct a Factor that Z-Scores each day's results.

        The Z-Score of a row is defined as::

            (row - row.mean()) / row.stddev()

        If ``mask`` is supplied, ignore values where ``mask`` returns False
        when computing row means and standard deviations, and output NaN
        anywhere the mask is False.

        If ``groupby`` is supplied, compute by partitioning each row based on
        the values produced by ``groupby``, z-scoring the partitioned arrays,
        and stitching the sub-results back together.

        Parameters
        ----------
        mask : ziplime.pipeline.Filter, optional
            A Filter defining values to ignore when Z-Scoring.
        groupby : ziplime.pipeline.Classifier, optional
            A classifier defining partitions over which to compute Z-Scores.

        Returns
        -------
        zscored : ziplime.pipeline.Factor
            A Factor producing that z-scores the output of self.

        Notes
        -----
        Mean and standard deviation are sensitive to the magnitudes of
        outliers. When working with factor that can potentially produce large
        outliers, it is often useful to use the ``mask`` parameter to discard
        values at the extremes of the distribution::

            >>> base = MyFactor(...)  # doctest: +SKIP
            >>> normalized = base.zscore(
            ...    mask=base.percentile_between(1, 99),
            ... )  # doctest: +SKIP

        ``zscore()`` is only supported on Factors of dtype float64.

        Examples
        --------
        See :meth:`~ziplime.pipeline.Factor.demean` for an in-depth
        example of the semantics for ``mask`` and ``groupby``.

        See Also
        --------
        :meth:`pandas.DataFrame.groupby`
        """
        from ziplime.pipeline.terms.factors.grouped_row_transform import GroupedRowTransform

        return GroupedRowTransform(
            transform=zscore,
            transform_args=(),
            factor=self,
            groupby=groupby,
            dtype=self.dtype,
            missing_value=self.missing_value,
            mask=mask,
            window_safe=True,
        )

    def rank(
            self, method="ordinal", ascending=True, mask=None, groupby=None
    ):
        """
        Construct a new Factor representing the sorted rank of each column
        within each row.

        Parameters
        ----------
        method : str, {'ordinal', 'min', 'max', 'dense', 'average'}
            The method used to assign ranks to tied elements. See
            `scipy.stats.rankdata` for a full description of the semantics for
            each ranking method. Default is 'ordinal'.
        ascending : bool, optional
            Whether to return sorted rank in ascending or descending order.
            Default is True.
        mask : ziplime.pipeline.Filter, optional
            A Filter representing assets to consider when computing ranks.
            If mask is supplied, ranks are computed ignoring any asset/date
            pairs for which `mask` produces a value of False.
        groupby : ziplime.pipeline.Classifier, optional
            A classifier defining partitions over which to perform ranking.

        Returns
        -------
        ranks : ziplime.pipeline.Factor
            A new factor that will compute the ranking of the data produced by
            `self`.

        Notes
        -----
        The default value for `method` is different from the default for
        `scipy.stats.rankdata`.  See that function's documentation for a full
        description of the valid inputs to `method`.

        Missing or non-existent data on a given day will cause an asset to be
        given a rank of NaN for that day.

        See Also
        --------
        :func:`scipy.stats.rankdata`
        """
        from ziplime.pipeline.terms.factors.grouped_row_transform import GroupedRowTransform
        from ziplime.pipeline.terms.factors.rank import Rank

        if groupby is None:
            return Rank(self, method=method, ascending=ascending, mask=mask)

        return GroupedRowTransform(
            transform=rankdata if ascending else rankdata_1d_descending,
            transform_args=(method,),
            factor=self,
            groupby=groupby,
            dtype=float64_dtype,
            missing_value=nan,
            mask=mask,
            window_safe=True,
        )

    @templated_docstring(CORRELATION_METHOD_NOTE=CORRELATION_METHOD_NOTE)
    def pearsonr(self, target: Term, correlation_length: int, mask: Filter | None = None):
        """
        Construct a new Factor that computes rolling pearson correlation
        coefficients between ``target`` and the columns of ``self``.

        Parameters
        ----------
        target : ziplime.pipeline.Term
            The term used to compute correlations against each column of data
            produced by `self`. This may be a Factor, a BoundColumn or a Slice.
            If `target` is two-dimensional, correlations are computed
            asset-wise.
        correlation_length : int
            Length of the lookback window over which to compute each
            correlation coefficient.
        mask : ziplime.pipeline.Filter, optional
            A Filter describing which assets should have their correlation with
            the target slice computed each day.

        Returns
        -------
        correlations : ziplime.pipeline.Factor
            A new Factor that will compute correlations between ``target`` and
            the columns of ``self``.

        Notes
        -----
        {CORRELATION_METHOD_NOTE}

        Examples
        --------
        Suppose we want to create a factor that computes the correlation
        between AAPL's 10-day returns and the 10-day returns of all other
        assets, computing each correlation over 30 days. This can be achieved
        by doing the following::

            returns = Returns(window_length=10)
            returns_slice = returns[sid(24)]
            aapl_correlations = returns.pearsonr(
                target=returns_slice, correlation_length=30,
            )

        This is equivalent to doing::

            aapl_correlations = RollingPearsonOfReturns(
                target=sid(24), returns_length=10, correlation_length=30,
            )

        See Also
        --------
        :func:`scipy.stats.pearsonr`
        :class:`ziplime.pipeline.factors.RollingPearsonOfReturns`
        :meth:`Factor.spearmanr`
        """
        from .statistical.rolling_pearson import RollingPearson

        return RollingPearson(
            base_factor=self,
            target=target,
            correlation_length=correlation_length,
            mask=mask,
        )

    @templated_docstring(CORRELATION_METHOD_NOTE=CORRELATION_METHOD_NOTE)
    def spearmanr(self, target: Term, correlation_length: int, mask: Filter | None = None):
        """
        Construct a new Factor that computes rolling spearman rank correlation
        coefficients between ``target`` and the columns of ``self``.

        Parameters
        ----------
        target : ziplime.pipeline.Term
            The term used to compute correlations against each column of data
            produced by `self`. This may be a Factor, a BoundColumn or a Slice.
            If `target` is two-dimensional, correlations are computed
            asset-wise.
        correlation_length : int
            Length of the lookback window over which to compute each
            correlation coefficient.
        mask : ziplime.pipeline.Filter, optional
            A Filter describing which assets should have their correlation with
            the target slice computed each day.

        Returns
        -------
        correlations : ziplime.pipeline.Factor
            A new Factor that will compute correlations between ``target`` and
            the columns of ``self``.

        Notes
        -----
        {CORRELATION_METHOD_NOTE}

        Examples
        --------
        Suppose we want to create a factor that computes the correlation
        between AAPL's 10-day returns and the 10-day returns of all other
        assets, computing each correlation over 30 days. This can be achieved
        by doing the following::

            returns = Returns(window_length=10)
            returns_slice = returns[sid(24)]
            aapl_correlations = returns.spearmanr(
                target=returns_slice, correlation_length=30,
            )

        This is equivalent to doing::

            aapl_correlations = RollingSpearmanOfReturns(
                target=sid(24), returns_length=10, correlation_length=30,
            )

        See Also
        --------
        :func:`scipy.stats.spearmanr`
        :meth:`Factor.pearsonr`
        """
        from .statistical.rolling_spearman import RollingSpearman

        return RollingSpearman(
            base_factor=self,
            target=target,
            correlation_length=correlation_length,
            mask=mask,
        )

    @templated_docstring(CORRELATION_METHOD_NOTE=CORRELATION_METHOD_NOTE)
    def linear_regression(self, target: Term, regression_length: int, mask: Filter | None = None):
        """
        Construct a new Factor that performs an ordinary least-squares
        regression predicting the columns of `self` from `target`.

        Parameters
        ----------
        target : ziplime.pipeline.Term
            The term to use as the predictor/independent variable in each
            regression. This may be a Factor, a BoundColumn or a Slice. If
            `target` is two-dimensional, regressions are computed asset-wise.
        regression_length : int
            Length of the lookback window over which to compute each
            regression.
        mask : ziplime.pipeline.Filter, optional
            A Filter describing which assets should be regressed with the
            target slice each day.

        Returns
        -------
        regressions : ziplime.pipeline.Factor
            A new Factor that will compute linear regressions of `target`
            against the columns of `self`.

        Notes
        -----
        {CORRELATION_METHOD_NOTE}

        Examples
        --------
        Suppose we want to create a factor that regresses AAPL's 10-day returns
        against the 10-day returns of all other assets, computing each
        regression over 30 days. This can be achieved by doing the following::

            returns = Returns(window_length=10)
            returns_slice = returns[sid(24)]
            aapl_regressions = returns.linear_regression(
                target=returns_slice, regression_length=30,
            )

        This is equivalent to doing::

            aapl_regressions = RollingLinearRegressionOfReturns(
                target=sid(24), returns_length=10, regression_length=30,
            )

        See Also
        --------
        :func:`scipy.stats.linregress`
        """
        from .statistical.rolling_linear_regression import RollingLinearRegression

        return RollingLinearRegression(
            dependent=self,
            independent=target,
            regression_length=regression_length,
            mask=mask,
        )

    #@float64_only
    def winsorize(
            self, min_percentile: int | float, max_percentile: int | float,
            mask: Filter | None = None, groupby: Classifier | None = None
    ):
        """
        Construct a new factor that winsorizes the result of this factor.

        Winsorizing changes values ranked less than the minimum percentile to
        the value at the minimum percentile. Similarly, values ranking above
        the maximum percentile are changed to the value at the maximum
        percentile.

        Winsorizing is useful for limiting the impact of extreme data points
        without completely removing those points.

        If ``mask`` is supplied, ignore values where ``mask`` returns False
        when computing percentile cutoffs, and output NaN anywhere the mask is
        False.

        If ``groupby`` is supplied, winsorization is applied separately
        separately to each group defined by ``groupby``.

        Parameters
        ----------
        min_percentile: float, int
            Entries with values at or below this percentile will be replaced
            with the (len(input) * min_percentile)th lowest value. If low
            values should not be clipped, use 0.
        max_percentile: float, int
            Entries with values at or above this percentile will be replaced
            with the (len(input) * max_percentile)th lowest value. If high
            values should not be clipped, use 1.
        mask : ziplime.pipeline.Filter, optional
            A Filter defining values to ignore when winsorizing.
        groupby : ziplime.pipeline.Classifier, optional
            A classifier defining partitions over which to winsorize.

        Returns
        -------
        winsorized : ziplime.pipeline.Factor
            A Factor producing a winsorized version of self.

        Examples
        --------
        .. code-block:: python

            price = USEquityPricing.close.latest
            columns={
                'PRICE': price,
                'WINSOR_1: price.winsorize(
                    min_percentile=0.25, max_percentile=0.75
                ),
                'WINSOR_2': price.winsorize(
                    min_percentile=0.50, max_percentile=1.0
                ),
                'WINSOR_3': price.winsorize(
                    min_percentile=0.0, max_percentile=0.5
                ),

            }

        Given a pipeline with columns, defined above, the result for a
        given day could look like:

        ::

                    'PRICE' 'WINSOR_1' 'WINSOR_2' 'WINSOR_3'
            Asset_1    1        2          4          3
            Asset_2    2        2          4          3
            Asset_3    3        3          4          3
            Asset_4    4        4          4          4
            Asset_5    5        5          5          4
            Asset_6    6        5          5          4

        See Also
        --------
        :func:`scipy.stats.mstats.winsorize`
        :meth:`pandas.DataFrame.groupby`
        """
        if not 0.0 <= min_percentile < max_percentile <= 1.0:
            raise BadPercentileBounds(
                min_percentile=min_percentile,
                max_percentile=max_percentile,
                upper_bound=1.0,
            )
        from ziplime.pipeline.terms.factors import GroupedRowTransform
        return GroupedRowTransform(
            transform=winsorize,
            transform_args=(min_percentile, max_percentile),
            factor=self,
            groupby=groupby,
            dtype=self.dtype,
            missing_value=self.missing_value,
            mask=mask,
            window_safe=self.window_safe,
        )

    def quantiles(self, bins: int, mask: Filter | None = None):
        """
        Construct a Classifier computing quantiles of the output of ``self``.

        Every non-NaN data point the output is labelled with an integer value
        from 0 to (bins - 1). NaNs are labelled with -1.

        If ``mask`` is supplied, ignore data points in locations for which
        ``mask`` produces False, and emit a label of -1 at those locations.

        Parameters
        ----------
        bins : int
            Number of bins labels to compute.
        mask : ziplime.pipeline.Filter, optional
            Mask of values to ignore when computing quantiles.

        Returns
        -------
        quantiles : ziplime.pipeline.Classifier
            A classifier producing integer labels ranging from 0 to (bins - 1).
        """
        if mask is None:
            mask = self.mask
        return Quantiles(inputs=(self,), bins=bins, mask=mask)

    def quartiles(self, mask: Filter | None = None):
        """
        Construct a Classifier computing quartiles over the output of ``self``.

        Every non-NaN data point the output is labelled with a value of either
        0, 1, 2, or 3, corresponding to the first, second, third, or fourth
        quartile over each row.  NaN data points are labelled with -1.

        If ``mask`` is supplied, ignore data points in locations for which
        ``mask`` produces False, and emit a label of -1 at those locations.

        Parameters
        ----------
        mask : ziplime.pipeline.Filter, optional
            Mask of values to ignore when computing quartiles.

        Returns
        -------
        quartiles : ziplime.pipeline.Classifier
            A classifier producing integer labels ranging from 0 to 3.
        """
        return self.quantiles(bins=4, mask=mask)

    def quintiles(self, mask: Filter | None = None):
        """
        Construct a Classifier computing quintile labels on ``self``.

        Every non-NaN data point the output is labelled with a value of either
        0, 1, 2, or 3, 4, corresonding to quintiles over each row.  NaN data
        points are labelled with -1.

        If ``mask`` is supplied, ignore data points in locations for which
        ``mask`` produces False, and emit a label of -1 at those locations.

        Parameters
        ----------
        mask : ziplime.pipeline.Filter, optional
            Mask of values to ignore when computing quintiles.

        Returns
        -------
        quintiles : ziplime.pipeline.Classifier
            A classifier producing integer labels ranging from 0 to 4.
        """
        return self.quantiles(bins=5, mask=mask)

    def deciles(self, mask: Filter | None = None):
        """
        Construct a Classifier computing decile labels on ``self``.

        Every non-NaN data point the output is labelled with a value from 0 to
        9 corresonding to deciles over each row.  NaN data points are labelled
        with -1.

        If ``mask`` is supplied, ignore data points in locations for which
        ``mask`` produces False, and emit a label of -1 at those locations.

        Parameters
        ----------
        mask : ziplime.pipeline.Filter, optional
            Mask of values to ignore when computing deciles.

        Returns
        -------
        deciles : ziplime.pipeline.Classifier
            A classifier producing integer labels ranging from 0 to 9.
        """
        return self.quantiles(bins=10, mask=mask)

    def top(self, N, mask=None, groupby=None):
        """
        Construct a Filter matching the top N asset values of self each day.

        If ``groupby`` is supplied, returns a Filter matching the top N asset
        values for each group.

        Parameters
        ----------
        N : int
            Number of assets passing the returned filter each day.
        mask : ziplime.pipeline.Filter, optional
            A Filter representing assets to consider when computing ranks.
            If mask is supplied, top values are computed ignoring any
            asset/date pairs for which `mask` produces a value of False.
        groupby : ziplime.pipeline.Classifier, optional
            A classifier defining partitions over which to perform ranking.

        Returns
        -------
        filter : ziplime.pipeline.Filter
        """
        if N == 1:
            # Special case: if N == 1, we can avoid doing a full sort on every
            # group, which is a big win.
            return self._maximum(mask=mask, groupby=groupby)
        return self.rank(ascending=False, mask=mask, groupby=groupby) <= N

    def bottom(self, N, mask=None, groupby=None):
        """
        Construct a Filter matching the bottom N asset values of self each day.

        If ``groupby`` is supplied, returns a Filter matching the bottom N
        asset values **for each group** defined by ``groupby``.

        Parameters
        ----------
        N : int
            Number of assets passing the returned filter each day.
        mask : ziplime.pipeline.Filter, optional
            A Filter representing assets to consider when computing ranks.
            If mask is supplied, bottom values are computed ignoring any
            asset/date pairs for which `mask` produces a value of False.
        groupby : ziplime.pipeline.Classifier, optional
            A classifier defining partitions over which to perform ranking.

        Returns
        -------
        filter : ziplime.pipeline.Filter
        """
        return self.rank(ascending=True, mask=mask, groupby=groupby) <= N

    def _maximum(self, mask=None, groupby=None):
        return MaximumFilter(self, groupby=groupby, mask=mask)

    def percentile_between(self, min_percentile, max_percentile, mask=None):
        """
        Construct a Filter matching values of self that fall within the range
        defined by ``min_percentile`` and ``max_percentile``.

        Parameters
        ----------
        min_percentile : float [0.0, 100.0]
            Return True for assets falling above this percentile in the data.
        max_percentile : float [0.0, 100.0]
            Return True for assets falling below this percentile in the data.
        mask : ziplime.pipeline.Filter, optional
            A Filter representing assets to consider when percentile
            calculating thresholds.  If mask is supplied, percentile cutoffs
            are computed each day using only assets for which ``mask`` returns
            True.  Assets for which ``mask`` produces False will produce False
            in the output of this Factor as well.

        Returns
        -------
        out : ziplime.pipeline.Filter
            A new filter that will compute the specified percentile-range mask.
        """
        return PercentileFilter(
            self,
            min_percentile=min_percentile,
            max_percentile=max_percentile,
            mask=mask,
        )

    #@if_not_float64_tell_caller_to_use_isnull
    def isnan(self):
        """
        A Filter producing True for all values where this Factor is NaN.

        Returns
        -------
        nanfilter : ziplime.pipeline.Filter
        """
        return self != self

    #@if_not_float64_tell_caller_to_use_isnull
    def notnan(self):
        """
        A Filter producing True for values where this Factor is not NaN.

        Returns
        -------
        nanfilter : ziplime.pipeline.Filter
        """
        return ~self.isnan()

    #@if_not_float64_tell_caller_to_use_isnull
    def isfinite(self):
        """
        A Filter producing True for values where this Factor is anything but
        NaN, inf, or -inf.
        """
        return (-inf < self) & (self < inf)

    def clip(self, min_bound, max_bound, mask=None):
        """
        Clip (limit) the values in a factor.

        Given an interval, values outside the interval are clipped to the
        interval edges. For example, if an interval of ``[0, 1]`` is specified,
        values smaller than 0 become 0, and values larger than 1 become 1.

        Parameters
        ----------
        min_bound : float
            The minimum value to use.
        max_bound : float
            The maximum value to use.
        mask : ziplime.pipeline.Filter, optional
            A Filter representing assets to consider when clipping.

        Notes
        -----
        To only clip values on one side, ``-np.inf` and ``np.inf`` may be
        passed.  For example, to only clip the maximum value but not clip a
        minimum value:

        .. code-block:: python

           factor.clip(min_bound=-np.inf, max_bound=user_provided_max)

        See Also
        --------
        numpy.clip
        """
        from .basic.clip import Clip

        return Clip(
            inputs=[self],
            min_bound=min_bound,
            max_bound=max_bound,
        )

    @classmethod
    def _principal_computable_term_type(cls):
        return Factor





