"""Click command group for backward-compatible sweep invocation."""

import asyncclick as click


class _SweepCommands(click.Group):
    """``ziplime sweep FILE`` runs FILE, as it did before ``sweep`` had subcommands."""

    def resolve_command(self, ctx: click.Context, args: list[str]):
        if args and args[0] not in self.commands and not args[0].startswith("-"):
            args = ["run", *args]
        return super().resolve_command(ctx, args)
