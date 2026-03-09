from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    from cli.app import main as app_main

    return app_main(argv)


__all__ = ["main"]
