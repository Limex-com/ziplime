import pytest

from ziplime.pipeline import domain
from ziplime.pipeline.data import Column, DataSet
from ziplime.pipeline.domain import AmbiguousDomain, Domain, GENERIC
from ziplime.pipeline.domain_inference import infer_domain


class Prices(DataSet):
    close = Column(float)


def test_legacy_domain_inference_import_remains_available():
    from ziplime.pipeline.domain import infer_domain as legacy_infer_domain

    assert legacy_infer_domain is infer_domain
    assert "infer_domain" in dir(domain)
    with pytest.raises(AttributeError, match="has no attribute 'unknown_domain_attribute'"):
        getattr(domain, "unknown_domain_attribute")


def test_empty_and_generic_inputs_infer_generic_domain():
    assert infer_domain([]) is GENERIC
    assert infer_domain([Prices.close]) is GENERIC


def test_generic_terms_do_not_override_one_concrete_domain():
    concrete = Domain()
    specialized = Prices.close.specialize(concrete)

    assert infer_domain(iter([Prices.close, specialized, specialized])) is concrete


def test_conflicting_domains_report_only_concrete_domains():
    first, second = Domain(), Domain()

    with pytest.raises(AmbiguousDomain) as caught:
        infer_domain([Prices.close, Prices.close.specialize(first), Prices.close.specialize(second)])

    assert set(caught.value.domains) == {first, second}
