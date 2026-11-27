"""Support for cross."""


#: Up to this many instruments ranks are counted pairwise, which is exactly Python's stable sort;
#: above it an argsort, where ties between equal float scores are not expected.
PAIRWISE = 64
