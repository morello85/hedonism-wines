"""Athena dashboard entrypoint.

This module preserves backwards compatibility with the existing
`data_viz_athena.py` implementation while exposing the preferred
`data_athena_viz.py` filename for orchestration.
"""

from data_viz_athena import main


if __name__ == "__main__":
    main()
