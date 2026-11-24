"""I python widget progress publisher for progress."""

from collections.abc import Iterable
from ziplime.utils.string_formatting import bulleted_list
from ziplime.pipeline import Term
from ziplime.pipeline.hooks.html_utils import repr_htmlsafe
from ziplime.pipeline.hooks.progress.progress_model import ProgressModel


try:
    import ipywidgets

    HAVE_WIDGETS = True

    # This VBox subclass exists to work around a strange display issue but
    # where the repr of the progress bar sometimes gets re-displayed upon
    # re-opening the notebook, even after the bar has closed. The repr of VBox
    # is somewhat noisy, so we replace it here with a version that just returns
    # an empty string.
    class ProgressBarContainer(ipywidgets.VBox):
        def __repr__(self) -> str:
            return ""

except ImportError:
    HAVE_WIDGETS = False


try:
    from IPython.display import display, HTML as IPython_HTML

    HAVE_IPYTHON = True
except ImportError:
    HAVE_IPYTHON = False


class IPythonWidgetProgressPublisher:
    """A progress publisher that publishes to an IPython/Jupyter widget."""

    def __init__(self) -> None:
        missing = []
        if not HAVE_WIDGETS:
            missing.append("ipywidgets")
        elif not HAVE_IPYTHON:
            missing.append("IPython")

        if missing:
            raise ValueError(
                "IPythonWidgetProgressPublisher needs ipywidgets and IPython:"
                "\nMissing:\n{}".format(bulleted_list(missing))
            )

        # Heading for progress display.
        self._heading = ipywidgets.HTML()

        # Percent Complete Indicator to the left of the bar.
        indicator_width = "120px"
        self._percent_indicator = ipywidgets.HTML(
            layout={"width": indicator_width},
        )

        # The progress bar itself.
        self._bar = ipywidgets.FloatProgress(
            value=0.0,
            min=0.0,
            max=100.0,
            bar_style="info",
            # Leave enough space for the percent indicator.
            layout={"width": "calc(100% - {})".format(indicator_width)},
        )
        bar_and_percent = ipywidgets.HBox([self._percent_indicator, self._bar])

        # Collapsable details tab underneath the progress bar.
        self._details_body = ipywidgets.HTML()
        self._details_tab = ipywidgets.Accordion(
            children=[self._details_body],
            selected_index=None,  # Start in collapsed state.
            layout={
                # Override default border settings to make details tab less
                # heavy.
                "border": "1px",
            },
        )
        # There's no public interface for setting title in the constructor :/.
        self._details_tab.set_title(0, "Details")

        # Container for the combined widget.
        self._layout = ProgressBarContainer(
            [
                self._heading,
                bar_and_percent,
                self._details_tab,
            ],
            # Overall layout consumes 75% of the page.
            layout={"width": "75%"},
        )

        self._displayed = False

    def publish(self, model: ProgressModel) -> None:
        if model.state == "init":
            self._heading.value = "<b>Analyzing Pipeline...</b>"
            self._set_progress(0.0)
            self._ensure_displayed()

        elif model.state in ("loading", "computing"):

            term_list = self._render_term_list(model.current_work)
            if model.state == "loading":
                details_heading = "<b>Loading Inputs:</b>"
            else:
                details_heading = "<b>Computing Expression:</b>"
            self._details_body.value = details_heading + term_list

            chunk_start, chunk_end = model.current_chunk_bounds
            self._heading.value = (
                "<b>Running Pipeline</b>: Chunk Start={}, Chunk End={}".format(
                    chunk_start.date(), chunk_end.date()
                )
            )

            self._set_progress(model.percent_complete)

            self._ensure_displayed()

        elif model.state == "success":
            # Replace widget layout with html that can be persisted.
            self._stop_displaying()
            display(
                IPython_HTML(
                    "<b>Pipeline Execution Time:</b> {}".format(
                        self._format_execution_time(model.execution_time)
                    )
                ),
            )

        elif model.state == "error":
            self._bar.bar_style = "danger"
            self._stop_displaying()
        else:
            self._layout.close()
            raise ValueError("Unknown display state: {!r}".format(model.state))

    def _ensure_displayed(self) -> None:
        if not self._displayed:
            display(self._layout)
            self._displayed = True

    def _stop_displaying(self) -> None:
        self._layout.close()

    @staticmethod
    def _render_term_list(terms: Iterable[Term]) -> str:
        list_elements = "".join(
            ["<li><pre>{}</pre></li>".format(repr_htmlsafe(t)) for t in terms]
        )
        return "<ul>{}</ul>".format(list_elements)

    def _set_progress(self, percent_complete: float) -> None:
        self._bar.value = percent_complete
        self._percent_indicator.value = "<b>{:.2f}% Complete</b>".format(
            percent_complete
        )

    @staticmethod
    def _format_execution_time(total_seconds: float):
        """Helper method for displaying total execution time of a Pipeline.

        Parameters
        ----------
        total_seconds : float
            Number of seconds elapsed.

        Returns
        -------
        formatted : str
            User-facing text representation of elapsed time.
        """

        def maybe_s(n: int) -> str:
            if n == 1:
                return ""
            return "s"

        minutes, seconds = divmod(total_seconds, 60)
        minutes = int(minutes)
        if minutes >= 60:
            hours, minutes = divmod(minutes, 60)
            t = "{hours} Hour{hs}, {minutes} Minute{ms}, {seconds:.2f} Seconds"
            return t.format(
                hours=hours,
                hs=maybe_s(hours),
                minutes=minutes,
                ms=maybe_s(minutes),
                seconds=seconds,
            )
        elif minutes >= 1:
            t = "{minutes} Minute{ms}, {seconds:.2f} Seconds"
            return t.format(
                minutes=minutes,
                ms=maybe_s(minutes),
                seconds=seconds,
            )
        else:
            return "{seconds:.2f} Seconds".format(seconds=seconds)
