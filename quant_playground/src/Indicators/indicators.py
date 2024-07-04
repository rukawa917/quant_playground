class Indicators:
    def __init__(self):
        pass
    
    def beta(self, returns, benchmark_returns):
        """
        calculate beta
        """
        cov = returns.cov(benchmark_returns)
        beta = cov / benchmark_returns.var()
        return beta
    
    def rolling_beta(self, returns, benchmark_returns, window):
        """
        calculate rolling beta
        """
        rolling_cov = returns.rolling(window).cov(benchmark_returns)
        rolling_var = benchmark_returns.rolling(window).var()
        rolling_beta = rolling_cov / rolling_var
        return rolling_beta
    