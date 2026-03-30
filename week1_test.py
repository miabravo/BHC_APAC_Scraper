import yfinance as yf


def fetch_company_info(ticker_label: str, yahoo_symbol: str):
    """
    Fetch basic company info (sector, industry, and full name) for a given
    Yahoo Finance symbol, while preserving the original ticker label for display.
    """
    ticker = yf.Ticker(yahoo_symbol)

    try:
        info = ticker.get_info()
    except Exception as e:
        print(f"[{ticker_label}] Error fetching data: {e}")
        return

    if not info:
        print(f"[{ticker_label}] No info returned from yfinance.")
        return

    long_name = info.get("longName", "N/A")
    sector = info.get("sector", "N/A")
    industry = info.get("industry", "N/A")

    print(f"=== {ticker_label} ===")
    print(f"Full Name : {long_name}")
    print(f"Sector    : {sector}")
    print(f"Industry  : {industry}")
    print()


def main():
    # Map the requested tickers to Yahoo Finance symbols
    ticker_map = {
        "TYO:4914": "4914.T",   # Shiseido Co., Ltd. on Tokyo Stock Exchange
        "NYSE:TMO": "TMO",      # Thermo Fisher Scientific Inc.
        "ASX:MSB": "MSB.AX",    # Mesoblast Limited on Australian Securities Exchange
    }

    for label, yahoo_symbol in ticker_map.items():
        fetch_company_info(label, yahoo_symbol)


if __name__ == "__main__":
    main()

import yfinance as yf

# Target tickers with Yahoo Finance's specific international formatting
tickers = ['4914.T', 'TMO', 'MSB.AX'] 

print("Fetching data, please wait...\n")

for ticker_symbol in tickers:
    try:
        company = yf.Ticker(ticker_symbol)
        info = company.info
        
        print(f"--- {ticker_symbol} ---")
        print(f"Name: {info.get('longName', 'Data not found')}")
        print(f"Sector: {info.get('sector', 'Data not found')}")
        print(f"Industry: {info.get('industry', 'Data not found')}\n")
        
    except Exception as e:
        print(f"Could not fetch data for {ticker_symbol}. Error: {e}\n")