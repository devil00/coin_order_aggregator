import argparse
import asyncio
import functools
import random
import time
import typing
from decimal import Decimal
from enum import Enum
from typing import Tuple

import httpx

Exchanges = Enum('Exchanges', "COINBASE GEMINI")

EXCHANGE_URLS = {Exchanges.COINBASE: "https://api.exchange.coinbase.com/products/BTC-USD/book?level=2",
                 Exchanges.GEMINI: "https://api.gemini.com/v1/book/BTCUSD"}

RATE_LIMIT_INTERVAL_SECONDS = 2
RATE_LIMIT_TOKEN = 1
EXCHANGE_TIMEOUT_SECONDS=10

parser = argparse.ArgumentParser(description="An order aggregator script that pulls live data from multiple crypto "
                                                 "exchanges and calculates the best execution price to buy or sell a"
                                                 " given quantity of Bitcoin.")
parser.add_argument("--qty", type=int, default=10, help="BTC Quantity to buy/sell")

def main():
    args = parser.parse_args()
    target_qty = args.qty
    bid_price, ask_price = asyncio.run(calculate_price(target_qty))
    print(f"To buy: {target_qty} BTC: {fmt_currency(ask_price)}")
    print(f"To sell: {target_qty} BTC: {fmt_currency(bid_price)}")

async def monitor():
    while True:
        tasks = asyncio.all_tasks()
        print(f"Active tasks: {[t.get_coro().__name__ for t in tasks]}")
        await asyncio.sleep(0.5)

def fmt_currency(value: Decimal, currency_symbol: str = "$") -> str:
    v = value.quantize(Decimal('0.01'))
    return f"{currency_symbol}{v:,.2f}"

async def calculate_price(qty) -> Tuple[Decimal, Decimal]:
    bids, asks = await extract_exchanges_orders()
    bid_total_cost, bid_qty_filled = await execute_order(bids, qty, 'bids')
    handle_incomplete_demanded_qty(qty, bid_qty_filled)

    ask_total_cost, ask_qty_filled = await execute_order(asks, qty, 'asks')
    handle_incomplete_demanded_qty(qty, ask_qty_filled)
    return bid_total_cost, ask_total_cost

def handle_incomplete_demanded_qty(target_qty, filled_qty):
    if filled_qty < Decimal(target_qty):
        print(f"Warning: only filled {filled_qty} of requested {target_qty}")

async def execute_order(orders, target_qty, transaction_type):
    """
    Execute a market order for a target quantity.

    :param orders: List of [price, size]; sorted ascending for asks, descending for bids
    :param target_qty: quantity to buy/sell
    :param transaction_type: 'buy' or 'sell'
    :return: total_cost
    """
    if not orders:
        return Decimal(0), Decimal(0)

    if transaction_type not in ('bids', 'asks'):
        raise ValueError("Invalid transaction type")

    orders = sorted(orders, key=lambda o: o[0], reverse=(transaction_type == 'bids'))

    remaining_qty = Decimal(str(target_qty))
    total_cost = Decimal("0")
    executed_qty = Decimal(0)

    for price, qty in orders:
        qty_taken = min(qty, remaining_qty)
        if qty_taken <= 0 or price <= 0:
            continue

        total_cost += price * qty_taken
        executed_qty += qty_taken
        remaining_qty -= qty_taken
        if remaining_qty <= 0:
            break

    await asyncio.sleep(0)

    return total_cost, executed_qty


async def extract_exchanges_orders() -> Tuple[typing.List, typing.List]:
    """
    Fetch exchange orders from exchange API.
    :return: (bids, asks)
    """
    bids = []
    asks = []
    orders = []
    exchange_error_count =0
    for coin_exchange in EXCHANGE_URLS.keys():
        try:
            json_data = await fetch_url(EXCHANGE_URLS[coin_exchange])

            if coin_exchange == Exchanges.GEMINI:
                orders = await gemini_orders(json_data)
            elif coin_exchange == Exchanges.COINBASE:
                orders = await coinbase_orders(json_data)
        except Exception as e:
            print(f"An error occurred: {e}")
            exchange_error_count += 1
            continue

        if orders:
            bids.extend(orders[0])
            asks.extend(orders[1])
    if exchange_error_count == len(EXCHANGE_URLS):
        raise Exception("Exchanges are down. Please try again.")

    return bids, asks


def rate_limiter(rate_per_sec: float, capacity: int):
    """
    Async rate limiter decorator (token bucket).
    rate_per_sec: tokens refilled per second
    capacity: max number of tokens in the bucket
    """
    tokens = capacity
    last_refill = time.monotonic()
    lock = asyncio.Lock()

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal tokens, last_refill
            async with lock:
                now = time.monotonic()
                elapsed = now - last_refill

                # Refill bucket
                refill = elapsed * rate_per_sec
                if refill > 0:
                    tokens = min(capacity, int(tokens + refill))
                    last_refill = now

                # If not enough tokens, throw error
                if tokens < 1:
                   raise Exception(f"Rate limit exceeded. Max {capacity} calls per {rate_per_sec} seconds.")

                # Consume one token
                tokens -= 1

            # Execute the wrapped function
            return await func(*args, **kwargs)
        return wrapper
    return decorator

def retry_with_backoff(retries=3, backoff_in_ms=100):
    def wrapper(f):
        @functools.wraps(f)
        async def wrapped(*args, **kwargs):
            x = 0
            while True:
                try:
                    return await f(*args, **kwargs)
                except Exception as e:
                    print('Fetch error:', e)

                    if x == retries or not _is_retryable_exceptions(e):
                        raise
                    else:
                        sleep_ms = (backoff_in_ms * 2 ** x +
                                    random.uniform(0, 1))
                        await asyncio.sleep(sleep_ms / 1000)
                        x += 1
                        print(f'Retrying {x + 1}/{retries}')

        return wrapped

    return wrapper

def _is_retryable_exceptions(ex) -> bool:
    return (isinstance(ex, httpx.HTTPStatusError)
            or isinstance(ex, httpx.RequestError)
            or isinstance(ex, httpx.TimeoutException))

async def coinbase_orders(json_data):
    def extract(json_key):
        return [[Decimal(trade[0]), Decimal(trade[1])] for trade in json_data[json_key]]

    if not json_data:
        return []
    return extract('bids'), extract('asks')


async def gemini_orders(json_data):
    def extract(json_key):
        return [[Decimal(trade['price']), Decimal(trade['amount'])] for trade in json_data[json_key]]

    if not json_data:
        return []
    return extract('bids'), extract('asks')


@rate_limiter(rate_per_sec=RATE_LIMIT_INTERVAL_SECONDS, capacity=RATE_LIMIT_TOKEN)
@retry_with_backoff(retries=2)
async def fetch_url(url):
    async with httpx.AsyncClient(timeout=EXCHANGE_TIMEOUT_SECONDS) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error {e.response.status_code} on {url}")
            raise e
        except httpx.RequestError as e:
            print(f"Request error on {url}: {e}")
            raise e

if __name__ == "__main__":
    main()