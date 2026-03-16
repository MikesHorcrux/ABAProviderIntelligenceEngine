from __future__ import annotations


def main(argv=None):
    from cli.app import main as cli_main

    return cli_main(argv)

__all__ = ["main"]
