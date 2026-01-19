from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors import CustomFactor


class IchimokuKinkoHyo(CustomFactor):
    """Compute the various metrics for the Ichimoku Kinko Hyo (Ichimoku Cloud).
    http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:ichimoku_cloud

    **Default Inputs:** :data:`ziplime.pipeline.data.EquityPricing.high`, \
                        :data:`ziplime.pipeline.data.EquityPricing.low`, \
                        :data:`ziplime.pipeline.data.EquityPricing.close`

    **Default Window Length:** 52

    Parameters
    ----------
    window_length : int > 0
        The length the the window for the senkou span b.
    tenkan_sen_length : int >= 0, <= window_length
        The length of the window for the tenkan-sen.
    kijun_sen_length : int >= 0, <= window_length
        The length of the window for the kijou-sen.
    chikou_span_length : int >= 0, <= window_length
        The lag for the chikou span.
    """  # noqa

    params = {
        "tenkan_sen_length": 9,
        "kijun_sen_length": 26,
        "chikou_span_length": 26,
    }
    inputs = (EquityPricing.high, EquityPricing.low, EquityPricing.close)
    outputs = (
        "tenkan_sen",
        "kijun_sen",
        "senkou_span_a",
        "senkou_span_b",
        "chikou_span",
    )
    window_length = 52

    def _validate(self):
        super(IchimokuKinkoHyo, self)._validate()
        for k, v in self.params.items():
            if v > self.window_length:
                raise ValueError(
                    "%s must be <= the window_length: %s > %s"
                    % (
                        k,
                        v,
                        self.window_length,
                    ),
                )

    def compute(
        self,
        today,
        assets,
        out,
        high,
        low,
        close,
        tenkan_sen_length,
        kijun_sen_length,
        chikou_span_length,
    ):

        out.tenkan_sen = tenkan_sen = (
            high[-tenkan_sen_length:].max(axis=0) + low[-tenkan_sen_length:].min(axis=0)
        ) / 2
        out.kijun_sen = kijun_sen = (
            high[-kijun_sen_length:].max(axis=0) + low[-kijun_sen_length:].min(axis=0)
        ) / 2
        out.senkou_span_a = (tenkan_sen + kijun_sen) / 2
        out.senkou_span_b = (high.max(axis=0) + low.min(axis=0)) / 2
        out.chikou_span = close[chikou_span_length]
