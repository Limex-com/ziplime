"""Asks for a dataset it never declared."""


async def initialize(context):
    context.universe = {"JNJ": await context.symbol("JNJ", mic="XNYS")}


def compute_signals(context, prices):
    return {"quality": prices.dataset("fundamentals").revenue}


async def handle_data(context, data):
    pass
