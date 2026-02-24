from numpy import empty_like, where

from ziplime.lib.normalize import naive_grouped_rowwise_apply
from ziplime.pipeline.terms.factors.factor import Factor
from ziplime.pipeline.terms.classifiers import Everything


class GroupedRowTransform(Factor):
    """
    A Factor that transforms an input factor by applying a row-wise
    shape-preserving transformation on classifier-defined groups of that
    Factor.

    This is most often useful for normalization operators like ``zscore`` or
    ``demean`` or for performing ranking using ``rank``.

    Parameters
    ----------
    transform : function[ndarray[ndim=1] -> ndarray[ndim=1]]
        Function to apply over each row group.
    factor : ziplime.pipeline.Factor
        The factor providing baseline data to transform.
    mask : ziplime.pipeline.Filter
        Mask of entries to ignore when calculating transforms.
    groupby : ziplime.pipeline.Classifier
        Classifier partitioning ``factor`` into groups to use when calculating
        means.
    transform_args : tuple[hashable]
        Additional positional arguments to forward to ``transform``.

    Notes
    -----
    Users should rarely construct instances of this factor directly.  Instead,
    they should construct instances via factor normalization methods like
    ``zscore`` and ``demean`` or using ``rank`` with ``groupby``.

    See Also
    --------
    ziplime.pipeline.Factor.zscore
    ziplime.pipeline.Factor.demean
    ziplime.pipeline.Factor.rank
    """

    window_length = 0

    def __new__(
            cls,
            transform,
            transform_args,
            factor,
            groupby,
            dtype,
            missing_value,
            mask,
            **kwargs,
    ):

        if mask is None:
            mask = factor.mask
        else:
            mask = mask & factor.mask

        if groupby is None:
            groupby = Everything(mask=mask)

        return super(GroupedRowTransform, cls).__new__(
            GroupedRowTransform,
            transform=transform,
            transform_args=transform_args,
            inputs=(factor, groupby),
            missing_value=missing_value,
            mask=mask,
            dtype=dtype,
            **kwargs,
        )

    def _init(self, transform, transform_args, *args, **kwargs):
        self._transform = transform
        self._transform_args = transform_args
        return super(GroupedRowTransform, self)._init(*args, **kwargs)

    @classmethod
    def _static_identity(cls, transform, transform_args, *args, **kwargs):
        return (
            super(GroupedRowTransform, cls)._static_identity(*args, **kwargs),
            transform,
            transform_args,
        )

    def _compute(self, arrays, dates, assets, mask):
        data = arrays[0]
        group_labels, null_label = self.inputs[1]._to_integral(arrays[1])
        # Make a copy with the null code written to masked locations.
        group_labels = where(mask, group_labels, null_label)
        return where(
            group_labels != null_label,
            naive_grouped_rowwise_apply(
                data=data,
                group_labels=group_labels,
                func=self._transform,
                func_args=self._transform_args,
                out=empty_like(data, dtype=self.dtype),
            ),
            self.missing_value,
        )

    @property
    def transform_name(self):
        return self._transform.__name__

    def graph_repr(self):
        """Short repr to use when rendering Pipeline graphs."""
        return type(self).__name__ + "(%r)" % self.transform_name
