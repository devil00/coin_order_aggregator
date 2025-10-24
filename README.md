**Objective**

Build a BTC-USD order book aggregator that pulls live data from multiple crypto exchanges and calculates the best execution price to buy or sell a given quantity of Bitcoin. This test evaluates your ability to work with real-world APIs, reason about raw data, and write clean, functional Python code.

**Task Requirements**
1. Fetches order books from two exchanges.


**Description**
1. Entry point to script `main` method.
2. Configure different exchange URLS in variable :EXCHANGE_URLS.
3. First method: `calculate_price` will be invoked that will fetch orders from all configured exchanges.
4. Second, `execute_order` method would compute the price of the given order quantity of BTC.
5. Explored api docs: <br>
a) https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book 
<br>
b) https://docs.gemini.com/rest/market-data#get-current-order-book


**Execution Instructions**
Requirements: Python 3.13.5 should be installed in your system OSX/Linux/Windows.

1. Install all the dependencies listed in `requirements.txt` file.
2. Run cmd: `python coin_order_aggregator.py`

**Assumptions**
1. If all exchanges are down, then throw an error , enforce client to retry after sometime.
2. If a single exchange is down. process order with available exchange.
3. If cli args qty is not entered, then default to 10 only to demonstrate working script.

**Further Improvements**
1. Poll Exchanges regularly to gather the order live data. 
2. Apply rate limiting per exchange.
3. Apply healthcheck for exchanges continuously.
4. Report exchange down error to be investigated manually by exposing metrics or emit failed events.
5. Add retry logic to fetch exchange data.
6. Print exchange name along with price, this will add better indicator of the trade.

