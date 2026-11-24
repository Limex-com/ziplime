"""Support for engine."""


#: How many bars MLX may queue before the host waits for them.
EVALUATE_EVERY = 32


ORDER_SLOTS = 4


SLOT_STEPS = (1, 2, 4)


BYTES_PER_ELEMENT_BAR = 1_600


QUEUE_BUDGET = 4e9


BARS_PER_CALL = 16
