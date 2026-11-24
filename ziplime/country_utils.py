"""Country-name to country-code conversion."""

from iso3166 import countries_by_name


def code(name: str) -> str:
    return countries_by_name[name].alpha2
