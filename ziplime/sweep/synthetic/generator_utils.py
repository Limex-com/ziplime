"""Generator utils for synthetic."""

import numpy as np
from ziplime.sweep.params import Family
from ziplime.sweep.synthetic.blocks import Blocks
from ziplime.sweep.synthetic.feature_values import Features
from ziplime.sweep.synthetic.garch import Garch
from ziplime.sweep.synthetic.gaussian import Gaussian
from ziplime.sweep.synthetic.base_generator import Generator
from ziplime.sweep.synthetic.iid import IID
from ziplime.sweep.synthetic.meboot_generator import Meboot
from ziplime.sweep.synthetic.shuffle import Shuffle
from ziplime.sweep.synthetic.stationary import Stationary


def generator(family: Family, feats: Features, pool: np.ndarray, start: int) -> Generator:
    arguments = (family, feats, pool, start)
    return {
        "iid": lambda: IID(*arguments),
        "shuffle": lambda: Shuffle(*arguments),
        "block": lambda: Blocks(*arguments, circular=False),
        "circular": lambda: Blocks(*arguments, circular=True),
        "stationary": lambda: Stationary(*arguments),
        "meboot": lambda: Meboot(*arguments),
        "gbm": lambda: Gaussian(*arguments, df=None),
        "student_t": lambda: Gaussian(*arguments, df=family.df),
        "garch": lambda: Garch(*arguments),
    }[family.method]()
