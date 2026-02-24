import datetime

import asyncclick as click



class _DatetimeParam(click.ParamType):
    def __init__(self, tz=None):
        self.tz = tz

    def parser(self, value):
        return value
        return datetime.datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=self.tz)

    @property
    def name(self):
        return type(self).__name__.upper()

    def convert(self, value, param, ctx):
        try:
            return self.parser(value)
        except ValueError:
            self.fail(
                "%s is not a valid %s" % (value, self.name.lower()),
                param,
                ctx,
            )


class Timestamp(_DatetimeParam):
    """A click parameter that parses the value into pandas.Timestamp objects.

    Parameters
    ----------
    tz : timezone-coercable, optional
        The timezone to parse the string as.
        By default the timezone will be infered from the string or naiive.
    """


class Date(_DatetimeParam):
    """A click parameter that parses the value into datetime.date objects.

    Parameters
    ----------
    tz : timezone-coercable, optional
        The timezone to parse the string as.
        By default the timezone will be infered from the string or naiive.
    as_timestamp : bool, optional
        If True, return the value as a pd.Timestamp object normalized to
        midnight.
    """

    def __init__(self, tz=None, as_timestamp=False):
        super(Date, self).__init__(tz=tz)
        self.as_timestamp = as_timestamp

    def parser(self, value):
        ts = super(Date, self).parser(value)
        return ts.normalize() if self.as_timestamp else ts.date()

