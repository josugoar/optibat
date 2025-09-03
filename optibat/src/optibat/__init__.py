from box import Box

from optibat.auth import login  # noqa: F401
from optibat.config import settings, update_config  # noqa: F401
from optibat.market import query_market
from optibat.metering import read_module
from optibat.model import run_model
from optibat.offer import quote_price
from optibat.output import write_output


def optibat(data: Box) -> Box:
    data = update_config(data)
    data = query_market(data)
    data = read_module(data)
    data = run_model(data)
    data = quote_price(data)
    data = write_output(data)
    return data
