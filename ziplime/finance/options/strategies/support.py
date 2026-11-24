"""Support for strategies."""


#: How far beyond the outermost strike the payoff is probed when deciding whether a structure's
#: profit is unbounded. Anything that is still rising 50% past the widest strike is rising forever:
#: the payoff is linear out there, so one point past the last kink settles it.
_UNBOUNDED_PROBE = 1.5
