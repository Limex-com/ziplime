"""Synthetic price paths made from the real bars, and the sweep run on every one of them.

A sweep chooses the combination that did best on one history -- the one that happened. Run on
paths that could have happened instead, the same grid shows which choices hold up: the
combination chosen on the paths can be looked at on the real bars, and the one chosen on the real
bars on the paths, family by family.

A path is a whole set of bars -- open, high, low, close, volume for every instrument -- from
``start_date`` on; the warm-up before it stays real, so every path starts where the real run
starts. Each bar of a path is built from one real bar: its close-to-close log return, its
opening gap, its high and low relative to its close, its volume. Methods that pick real bars pick
the same bar for every instrument, so the instruments move together as they did:

* ``iid`` -- bars drawn with replacement; ``shuffle`` -- without (a permutation)
* ``block`` -- blocks of ``block`` consecutive bars (moving block bootstrap, Künsch 1989);
  ``circular`` -- the same, wrapping around the end (Politis & Romano 1992); ``stationary`` --
  blocks of random length, ``block`` on average (Politis & Romano 1994). Blocks keep what a
  strategy may live on: trends, volatility clusters, mean reversion within the block
* ``meboot`` -- the maximum entropy bootstrap (Vinod 2004, as R's meboot): each instrument's log
  price redrawn from a density around its own values, in their order -- paths that keep the real
  path's shape, with its turns where they were
* ``gbm`` -- normal log returns with the real means and covariance; ``student_t`` -- the same with
  fat tails (a multivariate t with ``df`` degrees of freedom); the bars' shape and volume from
  real bars drawn at random
* ``garch`` -- GARCH(1,1) per instrument, fitted by maximum likelihood, driven by the real
  standardised residuals of a date drawn at random for all instruments at once (filtered
  historical simulation, Barone-Adesi et al. 1999): volatility clusters, fat tails and the
  correlation of the shocks, starting from the volatility at ``start_date``

Every combination then runs on every path the way it ran on the real bars -- the same converted
strategy, the same backend, every combination of a path in one call.
"""

from ziplime.sweep.synthetic.support import FIELDS  # noqa: F401
from ziplime.sweep.synthetic.support import METRICS  # noqa: F401
from ziplime.sweep.synthetic.bars import Bars  # noqa: F401
from ziplime.sweep.synthetic.bar_utils import side_by_side  # noqa: F401
from ziplime.sweep.synthetic.feature_values import Features  # noqa: F401
from ziplime.sweep.synthetic.feature_utils import features  # noqa: F401
from ziplime.sweep.synthetic.base_generator import Generator  # noqa: F401
from ziplime.sweep.synthetic.resampled import Resampled  # noqa: F401
from ziplime.sweep.synthetic.iid import IID  # noqa: F401
from ziplime.sweep.synthetic.shuffle import Shuffle  # noqa: F401
from ziplime.sweep.synthetic.blocks import Blocks  # noqa: F401
from ziplime.sweep.synthetic.stationary import Stationary  # noqa: F401
from ziplime.sweep.synthetic.meboot_generator import Meboot  # noqa: F401
from ziplime.sweep.synthetic.gaussian import Gaussian  # noqa: F401
from ziplime.sweep.synthetic.garch import Garch  # noqa: F401
from ziplime.sweep.synthetic.generator_utils import generator  # noqa: F401
from ziplime.sweep.synthetic.sampling_utils import meboot  # noqa: F401
from ziplime.sweep.synthetic.garch_utils import garch_variance  # noqa: F401
from ziplime.sweep.synthetic.garch_utils import fit_garch  # noqa: F401
from ziplime.sweep.synthetic.bar_utils import build  # noqa: F401
from ziplime.sweep.synthetic.bar_utils import as_bundle  # noqa: F401
from ziplime.sweep.synthetic.validation import refuse_splits  # noqa: F401
from ziplime.sweep.synthetic.utils import run  # noqa: F401
from ziplime.sweep.synthetic.logging_utils import quiet_logs  # noqa: F401
from ziplime.sweep.synthetic.report_utils import long_format  # noqa: F401
from ziplime.sweep.synthetic.report_utils import statistic  # noqa: F401
from ziplime.sweep.synthetic.report_utils import scores  # noqa: F401
from ziplime.sweep.synthetic.report_utils import ranks  # noqa: F401
from ziplime.sweep.synthetic.report_utils import spearman  # noqa: F401
from ziplime.sweep.synthetic.report_utils import summarize  # noqa: F401


__all__ = [
    'FIELDS',
    'METRICS',
    'Bars',
    'side_by_side',
    'Features',
    'features',
    'Generator',
    'Resampled',
    'IID',
    'Shuffle',
    'Blocks',
    'Stationary',
    'Meboot',
    'Gaussian',
    'Garch',
    'generator',
    'meboot',
    'garch_variance',
    'fit_garch',
    'build',
    'as_bundle',
    'refuse_splits',
    'run',
    'quiet_logs',
    'long_format',
    'statistic',
    'scores',
    'ranks',
    'spearman',
    'summarize',
]
