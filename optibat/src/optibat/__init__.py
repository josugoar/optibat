
"""
Optibat modeling and control subsystems for cross-market battery optimization.

Contains the definition for the main processing pipeline, which encompasses all
the XXXX_XXXX side steps required to assign a program to a battery.

Author: Josu Gomez Arana (XXXX_XXXX)
"""

from box import Box


# Importing modules at the top level intentionally immediately triggers
# the configuration loading side effects for the global settings and
# environments, ensuring that any code using this package, whether as a library
# or as an application, operates in the correct context. This guarantees that
# settings for XXXX_XXXX are always loaded and available, because XXXX_XXXX
# might change them whenever.
from optibat.auth import login  # noqa: F401
from optibat.config import settings, update_config  # noqa: F401
from optibat.market import query_market
from optibat.metering import read_module
from optibat.model import run_model
from optibat.offer import quote_price
from optibat.output import write_output


def optibat(data: Box) -> Box:
    """
    Executes the Optibat modeling and control subsystem pipeline for cross-market battery optimization.

    This WILL trigger side effects if automatic mode of operation is enabled.
    If using the XXXX_XXXX configuration, results WILL be written to the XXXX_XXXX!
    For non production, use the manual mode instead, where results will be available
    in the returned Box.

    The workflow is intentionally structured as a pipeline, where each step
    updates the shared data object. This design ensures that all relevant
    information is passed through each stage, allowing for flexible extension
    and debugging. Each function encapsulates a domain specific operation for
    configuration, market data retrieval, metering, modeling, offer generation,
    and output.

    Args:
        data (Box): Initial configuration and input data (see XXXX_XXXX for details).

    Returns:
        Box: The final data object, containing the results from all steps.
    """
    # The pipeline is strictly sequential because each step depends on the result of the previous one.
    # In principle, query_market and read_module could be parallelized, but currently there is a dependency
    # on market datetime data that requires query_market to run before read_module.
    data = update_config(data)
    data = query_market(data)
    data = read_module(data)
    data = run_model(data)
    data = quote_price(data)
    data = write_output(data)
    return data
