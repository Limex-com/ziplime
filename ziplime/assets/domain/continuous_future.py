from ziplime.utils.calendar_utils import get_calendar


class ContinuousFuture:
    """Represents a specifier for a chain of future contracts, where the
    coordinates for the chain are:
    root_symbol : str
        The root symbol of the contracts.
    offset : int
        The distance from the primary chain.
        e.g. 0 specifies the primary chain, 1 the secondary, etc.
    roll_style : str
        How rolls from contract to contract should be calculated.
        Currently supports 'calendar'.

    Instances of this class are exposed to the algorithm.
    """

    # cdef readonly int64_t sid
    # # Cached hash of self.sid
    # cdef int64_t sid_hash
    #
    # cdef readonly object root_symbol
    # cdef readonly int offset
    # cdef readonly object roll_style
    #
    # cdef readonly object start_date
    # cdef readonly object end_date
    #
    # cdef readonly object exchange_info
    #
    # cdef readonly object adjustment

    _kwargnames = frozenset({
        'sid',
        'root_symbol',
        'offset',
        'start_date',
        'end_date',
        'exchange',
    })

    def __init__(self,
                 sid, # sid is required
                 root_symbol,
                 offset,
                 roll_style,
                 start_date,
                 end_date,
                 exchange_info,
                 adjustment=None):

        self.sid = sid
        self.sid_hash = hash(sid)
        self.root_symbol = root_symbol
        self.roll_style = roll_style
        self.offset = offset
        self.exchange_info = exchange_info
        self.start_date = start_date
        self.end_date = end_date
        self.adjustment = adjustment

    @property
    def exchange(self):
        return self.exchange_info.canonical_name

    @property
    def exchange_full(self):
        return self.exchange_info.name

    def __int__(self):
        return self.sid

    def __index__(self):
        return self.sid

    def __hash__(self):
        return self.sid_hash

    def __str__(self):
        return '%s(%d [%s, %s, %s, %s])' % (
            type(self).__name__,
            self.sid,
            self.root_symbol,
            self.offset,
            self.roll_style,
            self.adjustment,
        )

    def __repr__(self):
        attrs = ('root_symbol', 'offset', 'roll_style', 'adjustment')
        tuples = ((attr, repr(getattr(self, attr, None)))
                  for attr in attrs)
        strings = ('%s=%s' % (t[0], t[1]) for t in tuples)
        params = ', '.join(strings)
        return 'ContinuousFuture(%d, %s)' % (self.sid, params)

    def __reduce__(self):
        """Function used by pickle to determine how to serialize/deserialize this
        class.  Should return a tuple whose first element is self.__class__,
        and whose second element is a tuple of all the attributes that should
        be serialized/deserialized during pickling.
        """
        return (self.__class__, (self.sid,
                                 self.root_symbol,
                                 self.start_date,
                                 self.end_date,
                                 self.offset,
                                 self.roll_style,
                                 self.exchange))

    def to_dict(self):
        """Convert to a python dict."""
        return {
            'sid': self.sid,
            'root_symbol': self.root_symbol,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'offset': self.offset,
            'roll_style': self.roll_style,
            'exchange': self.exchange,
        }

    @classmethod
    def from_dict(cls, dict_):
        """Build an ContinuousFuture instance from a dict."""
        return cls(**dict_)

    def is_alive_for_session(self, session_label):
        """Returns whether the continuous future is alive at the given dt.

        Parameters
        ----------
        session_label: datetime.datetime
            The desired session label to check. (midnight UTC)

        Returns
        -------
        boolean: whether the continuous is alive at the given dt.
        """
        ref_start = self.start_date.value
        ref_end = self.end_date.value

        return ref_start <= session_label.value <= ref_end

    def is_exchange_open(self, dt_minute):
        """

        Parameters
        ----------
        dt_minute: datetime.datetime (UTC, tz-aware)
            The minute to check.

        Returns
        -------
        boolean: whether the continuous futures's exchange is open at the
        given minute.
        """
        calendar = get_calendar(self.exchange)
        return calendar.is_open_on_minute(dt_minute)

