import pytest
import asyncio
import aiohttp
from typing import List, Dict, Optional

from dags.extract import extract_single_watch, extract_links, extract_watch_data

@pytest.mark.asyncio
async def test_extract_links_output():

    links = await extract_links(total_pages=1)  
    
    assert isinstance(links, list), "extract_links should return a list"
    if links:
        assert all(isinstance(link, str) for link in links), "All links should be strings"

@pytest.mark.asyncio
async def test_extract_single_watch_output():

    url = "https://www.ethoswatches.com/product-oris-aquis-01-733-7789-4157-07-8-23-04peb.html"
    
    async with aiohttp.ClientSession() as session:
        result = await extract_single_watch(session, url)
        
        assert isinstance(result, (dict, type(None))), "extract_single_watch should return either a dict or None"
        
        if isinstance(result, dict):
            assert 'name' in result, "Watch data should contain 'name'"
            assert 'price' in result, "Watch data should contain 'price'"

@pytest.mark.asyncio
async def test_extract_watch_data_output():

    test_urls = [
        "https://www.ethoswatches.com/product-girard-perregaux-laureato-81005-11s3320-1cm.html",
        "https://www.ethoswatches.com/product-omega-constellation-131-20-28-60-55-001.html",
        "https://www.ethoswatches.com/product-raymond-weil-freelancer-2490-scs-52051.html",
    ]
    
    results = await extract_watch_data(test_urls)

    assert None not in results, "Result list should not contain any None values"    
    assert isinstance(results, list), "extract_watch_data should return a list"
    if results:
        assert all(isinstance(item, dict) for item in results), "All items should be dictionaries"
        assert all('name' in item and 'price' in item for item in results), "Each dict should have name and price"
