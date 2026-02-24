from weakref import WeakKeyDictionary

from ziplime.pipeline.data.column import Column
from ziplime.pipeline.data.constants import IsSpecialization
from ziplime.pipeline.domain import GENERIC, Domain


class DataSetMeta(type):
    """
    Metaclass for DataSets

    Supplies name and dataset information to Column attributes, and manages
    families of specialized dataset.
    """

    def __new__(metacls, name, bases, dict_):
        if len(bases) != 1:
            # Disallowing multiple inheritance makes it easier for us to
            # determine whether a given dataset is the root for its family of
            # specializations.
            raise TypeError("Multiple dataset inheritance is not supported.")

        # This marker is set in the class dictionary by `specialize` below.
        is_specialization = dict_.pop(IsSpecialization, False)

        newtype = super(DataSetMeta, metacls).__new__(metacls, name, bases, dict_)

        if not isinstance(newtype.domain, Domain):
            raise TypeError(
                "Expected a Domain for {}.domain, but got {} instead.".format(
                    newtype.__name__,
                    type(newtype.domain),
                )
            )

        # Collect all of the column names that we inherit from our parents.
        column_names = set().union(
            *(getattr(base, "_column_names", ()) for base in bases)
        )

        # Collect any new columns from this dataset.
        for maybe_colname, maybe_column in dict_.items():
            if isinstance(maybe_column, Column):
                # add column names defined on our class
                bound_column_descr = maybe_column.bind(maybe_colname)
                setattr(newtype, maybe_colname, bound_column_descr)
                column_names.add(maybe_colname)

        newtype._column_names = frozenset(column_names)

        if not is_specialization:
            # This is the new root of a family of specializations. Store the
            # memoized dictionary for family on this type.
            newtype._domain_specializations = WeakKeyDictionary(
                {
                    newtype.domain: newtype,
                }
            )

        return newtype

    def specialize(cls, domain: Domain):
        """
        Specialize a generic DataSet to a concrete domain.

        Parameters
        ----------
        domain : ziplime.pipeline.domain.Domain
            Domain to which we should generate a specialization.

        Returns
        -------
        specialized : type
            A new :class:`~ziplime.pipeline.data.DataSet` subclass with the
            same columns as ``self``, but specialized to ``domain``.
        """
        # We're already the specialization to this domain, so just return self.
        if domain == cls.domain:
            return cls

        try:
            return cls._domain_specializations[domain]
        except KeyError as exc:
            if not cls._can_create_new_specialization(domain):
                # This either means we're already a specialization and trying
                # to create a new specialization, or we're the generic version
                # of a root-specialized dataset, which we don't want to create
                # new specializations of.
                raise ValueError(
                    "Can't specialize {dataset} from {current} to new domain {new}.".format(
                        dataset=cls.__name__,
                        current=cls.domain,
                        new=domain,
                    )
                ) from exc
            new_type = cls._create_specialization(domain)
            cls._domain_specializations[domain] = new_type
            return new_type

    def unspecialize(cls):
        """
        Unspecialize a dataset to its generic form.

        This is equivalent to ``dataset.specialize(GENERIC)``.
        """
        return cls.specialize(GENERIC)

    def _can_create_new_specialization(cls, domain):
        # Always allow specializing to a generic domain.
        if domain is GENERIC:
            return True
        elif "_domain_specializations" in vars(cls):
            # This branch is True if we're the root of a family.
            # Allow specialization if we're generic.
            return cls.domain is GENERIC
        else:
            # If we're not the root of a family, we can't create any new
            # specializations.
            return False

    def _create_specialization(cls, domain):
        # These are all assertions because we should have handled these cases
        # already in specialize().
        assert isinstance(domain, Domain)
        assert (
                domain not in cls._domain_specializations
        ), "Domain specializations should be memoized!"
        if domain is not GENERIC:
            assert (
                    cls.domain is GENERIC
            ), "Can't specialize dataset with domain {} to domain {}.".format(
                cls.domain,
                domain,
            )

        # Create a new subclass of ``self`` with the given domain.
        # Mark that it's a specialization so that we know not to create a new
        # family for it.
        name = cls.__name__
        bases = (cls,)
        dict_ = {"domain": domain, IsSpecialization: True}
        out = type(name, bases, dict_)
        out.__module__ = cls.__module__
        return out

    @property
    def columns(cls):
        return frozenset(getattr(cls, colname) for colname in cls._column_names)

    @property
    def qualname(cls):
        if cls.domain is GENERIC:
            specialization_key = ""
        elif cls.domain.assets:
            specialization_key = f"sids<{[asset.sid for asset in cls.domain.assets]}>"
        else:
            specialization_key = "<" + cls.domain.country_code + ">"

        return cls.__name__ + specialization_key

    # NOTE: We used to use `functools.total_ordering` to account for all of the
    #       other rich comparison methods, but it has issues in python 3 and
    #       this method is only used for test purposes, so for now we will just
    #       keep this in isolation. If we ever need any of the other comparison
    #       methods we will have to implement them individually.
    def __lt__(cls, other):
        return id(cls) < id(other)

    def __repr__(cls):
        return "<DataSet: %r, domain=%s>" % (cls.__name__, cls.domain)
