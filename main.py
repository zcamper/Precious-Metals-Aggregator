import asyncio
import os

from apify import Actor
from apify_client import ApifyClientAsync

# All precious metals dealer actors
DEALERS = {
    'JM Bullion':       'iFEHhJHzudQlgUndW',
    'SD Bullion':       'IoQZHQMDEc5APLdur',
    'APMEX':            '7ZgRSUq0kTFSYIVFu',
    'Hero Bullion':     'SqTz0HOSCx2SnOP1a',
    'Silver.com':       'FTlTaQthalz9pMQIa',
    'Provident Metals': 'AIebTMyfaMnmKCLo7',
    'Monument Metals':  'NkaXVsCWj5wnaDJVl',
    'BGASC':            'oQzl49thH7hyt7RhE',
    'ModernCoinMart':   'Ii5FyOQm9ZHzkmPzg',
    'Kitco':            'uZC7bYm5bQKDhaxF8',
    'GoldSilver.com':   'vElSO8MZ9MsSC72T6',
}

# JM Bullion uses Playwright (needs more memory), all others use lightweight curl_cffi
HEAVY_DEALERS = {'JM Bullion'}


async def run_dealer(client: ApifyClientAsync, dealer_name: str, actor_id: str,
                     search_terms: list[str], max_items: int) -> list[dict]:
    """Run a single dealer's scraper and return results with dealer name added."""
    Actor.log.info(f"Starting {dealer_name} scraper (actor {actor_id})...")

    try:
        run = await client.actor(actor_id).call(
            run_input={
                'search_terms': search_terms,
                'max_items': max_items,
            },
            timeout_secs=300,
            memory_mbytes=4096 if dealer_name in HEAVY_DEALERS else 512,
        )

        run_status = run.get('status', 'UNKNOWN')
        if run_status != 'SUCCEEDED':
            Actor.log.warning(f"{dealer_name} scraper finished with status: {run_status}")
            return []

        dataset_id = run.get('defaultDatasetId')
        if not dataset_id:
            Actor.log.warning(f"{dealer_name}: No dataset ID found")
            return []

        items_response = await client.dataset(dataset_id).list_items()
        items = items_response.items if items_response else []

        Actor.log.info(f"{dealer_name}: Got {len(items)} products")

        # Add dealer name to each item
        for item in items:
            item['dealer'] = dealer_name

        return items

    except Exception as e:
        Actor.log.error(f"{dealer_name} scraper failed: {e}")
        return []


async def main():
    async with Actor:
        actor_input = await Actor.get_input() or {}
        search_terms = actor_input.get('search_terms', ['Silver coin'])
        max_items_per_dealer = actor_input.get('max_items_per_dealer', 5)
        selected_dealers = actor_input.get('dealers', [])

        # Filter dealers if specific ones were requested
        dealers_to_run = {}
        if selected_dealers:
            for name, actor_id in DEALERS.items():
                if any(d.lower() in name.lower() for d in selected_dealers):
                    dealers_to_run[name] = actor_id
            if not dealers_to_run:
                Actor.log.warning(f"No matching dealers found for: {selected_dealers}. Running all.")
                dealers_to_run = DEALERS
        else:
            dealers_to_run = DEALERS

        Actor.log.info(
            f"Starting Precious Metals Aggregator: "
            f"{len(dealers_to_run)} dealers, "
            f"search_terms={search_terms}, "
            f"max_items_per_dealer={max_items_per_dealer}"
        )

        token = os.environ.get('APIFY_TOKEN', '')
        client = ApifyClientAsync(token=token)

        # Run all dealer scrapers concurrently
        tasks = []
        for dealer_name, actor_id in dealers_to_run.items():
            task = run_dealer(client, dealer_name, actor_id, search_terms, max_items_per_dealer)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect all products and push to dataset
        total_products = 0
        for result in results:
            if isinstance(result, Exception):
                Actor.log.error(f"Dealer task exception: {result}")
                continue
            if not result:
                continue

            for item in result:
                await Actor.push_data(item)
                total_products += 1

        Actor.log.info(
            f"Aggregation complete. "
            f"Total products: {total_products} from {len(dealers_to_run)} dealers"
        )


if __name__ == '__main__':
    asyncio.run(main())
