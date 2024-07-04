"""
Module for data manager class
"""
from typing import List, Dict, Literal
import os
import pandas as pd


class DataManager:
    """
    Data Manager class to manage data loading, saving, and processing
    """
    def __init__(self, data_path):
        self.data_path = data_path

    def list_universe(self):
        """
        method to list all the symbols in the data path
        """
        f_list: List[str] = os.listdir(self.data_path)
        symbols = [f.split('.')[0] for f in f_list if f.endswith('.parquet') and not '_' in f and 'USDT' in f]
        symbols = sorted(symbols)
        return symbols

    def load_data(self, symbol: str, interval: str):
        """
        load ohlcv data for a given symbol and interval
        """
        target_data_path = os.path.join(self.data_path, f'{symbol}.parquet')
        data = pd.read_parquet(target_data_path)
        # check if interval exists in the data
        if interval not in data['interval'].unique():
            raise ValueError(f'Interval {interval} not found in the data')
        data = data[data['interval'] == interval]
        data = data.reset_index(drop=True)
        return data

    def prep_ohlcv_data(self, symbols: List[str], interval: str) -> Dict[str, pd.DataFrame]:
        """
        prepare data for multiple symbols in a dictionary
        """
        data = {symbol: self.load_data(symbol, interval) for symbol in symbols}
        return data

    def prep_return_data(self, ohlcv_dict: Dict[str, pd.DataFrame], style: Literal['close', 'ohlc']):
        """
        prepare return data in different styles
        """
        returns = pd.DataFrame()

        for symbol, df in ohlcv_dict.items():
            match style:
                case 'close':
                    new_price = df['close']
                case 'ohlc':
                    new_price = (df['close'] + df['open'] + df['high'] + df['low']) / 4
                case _:
                    raise ValueError(f'Unknown style: {style}')

            tmp_return_df = new_price.pct_change().dropna().to_frame(name=symbol)
            tmp_return_df.set_index(df['open_ts'].iloc[1:], inplace=True)
            returns = pd.merge(returns, tmp_return_df, how='outer', left_index=True, right_index=True)
        returns = returns.T.sort_index().T
        returns.dropna(inplace=True)

        # drop columns where the values are all 0
        returns = returns.loc[:, (returns != 0).any(axis=0)]

        return returns
