"""Raises on its third bar and every bar after."""
async def initialize(context):
    context.bars = 0


async def handle_data(context, data):
    context.bars += 1
    if context.bars >= 3:
        raise RuntimeError("broken strategy")
