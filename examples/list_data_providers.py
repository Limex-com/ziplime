"""Show which data connectors this build ships and which are ready to use.

Connectors are discovered, not hardcoded: removing a connector's package leaves an
installation that still ships the others, and this script keeps working.
``ziplime.data_providers`` entry point shows up here without any change to ziplime.
"""
from ziplime.utils.bundle_utils import list_providers


def main():
    print(f"{'provider':10s} {'assets':7s} {'bars':6s} {'calendar':9s} {'ready':6s}  description")
    for provider in list_providers():
        ready = "yes" if provider.is_configured else "no"
        print(f"{provider.name:10s} {str(provider.supports_assets):7s} "
              f"{str(provider.supports_market_data):6s} {str(provider.default_calendar):9s} "
              f"{ready:6s}  {provider.description}")
        if not provider.is_configured:
            print(f"{'':10s} set: {', '.join(provider.missing_env())}")


if __name__ == "__main__":
    main()
