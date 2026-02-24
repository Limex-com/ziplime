from numpy import unique, copyto

from ziplime.pipeline.mixins import SingleInputMixin
from ziplime.pipeline.terms.factors.custom_factor import CustomFactor


class PeerCount(SingleInputMixin, CustomFactor):
    """
    Peer Count of distinct categories in a given classifier.  This factor
    is returned by the classifier instance method peer_count()

    **Default Inputs:** None

    **Default Window Length:** 1
    """

    window_length = 1

    def _validate(self):
        super(PeerCount, self)._validate()
        if self.window_length != 1:
            raise ValueError(
                "'PeerCount' expected a window length of 1, but was given"
                "{window_length}.".format(window_length=self.window_length)
            )

    def compute(self, today, assets, out, classifier_values):
        # Convert classifier array to group label int array
        group_labels, null_label = self.inputs[0]._to_integral(classifier_values[0])
        _, inverse, counts = unique(  # Get counts, idx of unique groups
            group_labels,
            return_counts=True,
            return_inverse=True,
        )
        copyto(out, counts[inverse], where=(group_labels != null_label))