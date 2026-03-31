import yfinance as yf


def fetch_company_info(ticker_symbol: str) -> None:
    """
    Fetch and print company info for one Yahoo Finance ticker.
    If retrieval fails, print a warning and continue.
    """
    ticker = yf.Ticker(ticker_symbol)

    try:
        info = ticker.get_info()
    except Exception as e:
        print(f"[WARNING] Could not fetch data for {ticker_symbol}: {e}\n")
        return

    if not info:
        print(f"[WARNING] No info returned for {ticker_symbol}.\n")
        return

    long_name = info.get("longName", "N/A")
    sector = info.get("sector", "N/A")
    industry = info.get("industry", "N/A")
    market_cap = info.get("marketCap", "N/A")

    print(f"=== {ticker_symbol} ===")
    print(f"Company Name : {long_name}")
    print(f"Sector    : {sector}")
    print(f"Industry  : {industry}")
    print(f"Market Cap: {market_cap}")
    print()


def main() -> None:
    tickers = [
        "4914.T",
        "LONN.SW",
        "688238.SS",
        "TMO",
        "MSB.AX",
        "078160.KQ",
        "4978.T",
        "4527.T",
        "CYP.AX",
        "298060.KQ",
        "BIO",
        "6869.T",
        "AZT.OL",
        "MRK.DE",
        "688105.SS",
        "301047.SZ",
    ]

    print("Fetching company info from Yahoo Finance...\n")
    for ticker_symbol in tickers:
        fetch_company_info(ticker_symbol)


if __name__ == "__main__":
    main()