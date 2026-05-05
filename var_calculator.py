import numpy as np
from collections import deque

class PriceWindow:
    def __init__(self, ticker):
        self.ticker = ticker
        self.window = deque(maxlen=50)
    
    def add_price(self, price):
        self.window.append(price)
    
    def calculate_var(self, confidence=0.95):
        if len(self.window)<2:
            return None
        rets = []
        for i in range(len(self.window)-1):
            ret = (self.window[i+1]-self.window[i])/self.window[i]
            rets.append(ret)
        rets.sort()
        VaR = np.percentile(rets, 5)
        return VaR

class PortfolioVaR:
    def __init__(self):
        
        self.tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        self.windows = {ticker: PriceWindow(ticker) for ticker in self.tickers}
        self.ticker_vars = {}

    def update(self, ticker, price):
        self.windows[ticker].add_price(price)
        self.ticker_vars[ticker] = self.windows[ticker].calculate_var()
        
    
    def calculate_portfolio_var(self):
        return sum(var for var in self.ticker_vars.values() if var is not None)
    
    def check_threshold(self, threshold=-0.02):
        portfolio_var = self.calculate_portfolio_var()
        if portfolio_var < threshold:
            print(f'ALERT: Portfolio VaR {portfolio_var: .4f} exceed threshold!!')