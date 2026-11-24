"""HTML-safe representations for pipeline progress."""

from ziplime.utils.compat import escape_html


def repr_htmlsafe(t):
    """Repr a value and html-escape the result.

    If an error is thrown by the repr, show a placeholder.
    """
    try:
        r = repr(t)
    except Exception:
        r = "(Error Displaying {})".format(type(t).__name__)

    return escape_html(str(r), quote=True)
