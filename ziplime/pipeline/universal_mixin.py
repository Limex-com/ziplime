"""Shared specialization and cache for pipeline term wrapper mixins."""

from abc import abstractmethod

from ziplime.pipeline.terms.term import Term


class UniversalMixin(Term):
    """
    Base class for "universal" mixins.

    Universal mixins are used to implement expressions that need separate
    subclasses for each of the ComputableTerm subclasses (Factor, Filter, and
    Classifier). Such expressions are usually return types of methods of
    ComputableTerm, such as `downsample()`, `alias()`, or `fillna()`.

    A type may only inherit from one UniversalMixin.
    """

    # Memo dict mapping pairs of (mixin_type, principal_type) to subtypes.
    _UNIVERSAL_MIXIN_SUBTYPES = {}

    @staticmethod
    @abstractmethod
    def _universal_mixin_type() -> 'type[UniversalMixin]':
        raise NotImplementedError("_universal_mixin_type")

    @staticmethod
    @abstractmethod
    def _universal_mixin_specialization_name(principal_type: type[Term]) -> str:
        raise NotImplementedError("_universal_mixin_specialization_name")

    @classmethod
    def universal_mixin_specialization[T: Term](cls, principal_type: type[T]) -> type[T]:
        """
        Create a new subtype of `principal_type` that adds this mixin to
        ``principal_type``. ``principal_type`` will be one of Factor, Filter,
        or Classifier.
        """
        mixin = cls._universal_mixin_type()
        memo_key = (mixin, principal_type)

        try:
            return cls._UNIVERSAL_MIXIN_SUBTYPES[memo_key]
        except KeyError:
            new_type = type(
                mixin._universal_mixin_specialization_name(principal_type),
                (mixin, principal_type),
                {"__module__": principal_type.__module__},
            )
            cls._UNIVERSAL_MIXIN_SUBTYPES[memo_key] = new_type
            return new_type
