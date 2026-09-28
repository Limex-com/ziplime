

def calculate_per_unit_commission(
    order, transaction, cost_per_unit, initial_commission, min_trade_cost
):
    """
    If there is a minimum commission:
        If the order hasn't had a commission paid yet, pay the minimum
        commission.

        If the order has paid a commission, start paying additional
        commission once the minimum commission has been reached.

    If there is no minimum commission:
        Pay commission based on number of units in the transaction.
    """
    additional_commission = abs(transaction.amount * cost_per_unit)

    if order.commission == 0:
        # no commission paid yet, pay at least the minimum plus a one-time
        # exchange fee.
        return max(min_trade_cost, additional_commission + initial_commission)
    else:
        # we've already paid some commission, so figure out how much we
        # would be paying if we only counted per unit.
        per_unit_total = (
            abs(order.filled * cost_per_unit)
            + additional_commission
            + initial_commission
        )

        if per_unit_total < min_trade_cost:
            # if we haven't hit the minimum threshold yet, don't pay
            # additional commission
            return 0
        else:
            # we've exceeded the threshold, so pay more commission.
            return per_unit_total - order.commission
