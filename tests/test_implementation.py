from bluesky5f61dce7fa034 import query
from exorde_data.models import Item
import pytest


@pytest.mark.asyncio
async def test_query():
    try:
        parameters = {
            "max_oldness_seconds": 3600,
            "maximum_items_to_collect": 10,
            "url_parameters": {"keyword": "hey"}
        }
        async for post in query(parameters):
            print(post)
    except ValueError as e:
        print(f"Error: {str(e)}")

import asyncio
asyncio.run(test_query())