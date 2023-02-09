"""The file contains the CLI interface for the pytemplate package."""
import click
from cprint import cprint


@click.group(invoke_without_command=True)
@click.pass_context
def main(ctx):
    """
    Main class for CLI access

    Args:
        ctx (context): Click context
    """    
    ctx.ensure_object(dict)
    if ctx.invoked_subcommand is None:
        ctx.invoke(echo)


@main.command()
@click.argument("string_arg", nargs=1, required=False)
@click.option(
    "--string",
    "-s",
    default="Hello World!",
    required=False,
)
@click.option(
    "--color",
    "-c",
    default="ok",
    help="Output color (info, ok, warn, err)",
    required=False,
)
@click.pass_context
def echo(ctx, string, string_arg, color):
    """
    Echo a string with color

    Args:
        ctx (click.Context): The click context
        string (str): The string to echo (-s)
        string_arg (str): The string to echo (positional)
        color (str): The color to use (info, ok, warn, err)
    """
    prt = string_arg or string
    getattr(cprint, color)(prt)


if __name__ == "__main__":
    main()
