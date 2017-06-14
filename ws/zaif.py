import json
from abc import abstractmethod
from collections import deque

import numpy as np
import pandas as pd
import websockets

from .websocket import WebSocketParent


class ZaifWebSocketParent(WebSocketParent):
    """Zaif WebSocket Base Class for orderbooks APIs."""

    def __init__(self):
        super().__init__()
        self.currency_pair = None

    async def on_connect(self) -> None:
        async with websockets.connect(f'wss://ws.zaif.jp:8888/stream?currency_pair={self.currency_pair}') as ws:
            self.logger.info('Start to subscribe')
            while True:
                msg = json.loads(await ws.recv())
                await self.on_message(msg)


    async def subscribe(self, ws) -> None:
        pass

    @abstractmethod
    async def on_message(self, msg) -> None:
        """Processes of callback on message receiving."""



class ZaifWebSocketBTCJPY(ZaifWebSocketParent):
    """
    Fetch Trades(Executions) Data from Zaif WebSocket API.
    This Class calculate Bolinger bands and if the new price breaks the bands, 
    Tells it in console message.
    """

    def __init__(self, window_size: pd.Timedelta, sigma: int):
        super().__init__()
        self.currency_pair = 'btc_jpy'
        self.order_dict = {'ask_time': deque([pd.Timestamp('2000-01-01')]), 'ask_price': deque([1.]), 'ask_volume': deque([1.]),
                           'bid_time': deque([pd.Timestamp('2000-01-01')]),'bid_price': deque([1.]), 'bid_volume': deque([1.])}
        self.window_size = window_size
        self.sigma = sigma
        self.upper_band = None
        self.lower_band = None
        self.enough_gathered = {'ask': False, 'bid': False}

    async def on_message(self, msg):
        time = pd.Timestamp.now()
        ask_price, ask_volume, bid_price, bid_volume = self._parse_trade(msg)
        if ask_price != self.order_dict['ask_price'][-1] and ask_volume != self.order_dict['ask_volume'][-1]:
            self._update_order_dict(time, ask_price, ask_volume, 'ask')
            self._get_bolingers()
            if self.enough_gathered['ask'] and ask_price < self.lower_band:
                self.logger.info(f'Break Lower Band | Ask: {ask_price} LowerBand: {self.lower_band}')
        if bid_price != self.order_dict['bid_price'][-1] and bid_volume != self.order_dict['bid_volume'][-1]:
            self._update_order_dict(time, bid_price, bid_volume, 'bid')
            self._get_bolingers()
            if self.enough_gathered['bid'] :#and bid_price < self.upper_band:
                self.logger.info(f'Break Upper Band | Bid: {bid_price} UpperBand: {self.upper_band}')

    def _update_order_dict(self, time, price, volume, side):
        for label, content in zip((f'{side}_price', f'{side}_volume', f'{side}_time'), (price, volume, time)):
            self.order_dict[label].append(content)
        last = self.order_dict[f'{side}_time'][-1]
        while True:
            first = self.order_dict[f'{side}_time'][0]
            if last - first < self.window_size:
                break
            for key in [f'{side}_price', f'{side}_volume', f'{side}_time']:
                self.order_dict[key].popleft()
        self._check_order_dict_num()

    def _get_bolingers(self):
        ask_mean, bid_mean = np.mean(self.order_dict['ask_price']), np.mean(self.order_dict['bid_price'])
        ask_std, bid_std = np.std(self.order_dict['ask_price']), np.std(self.order_dict['bid_price'])
        self.upper_band = bid_mean + bid_std
        self.lower_band = ask_mean - ask_std

    def _check_order_dict_num(self, minimum_num=10):
        for side in ['ask', 'bid']:
            if len(self.order_dict[f'{side}_price']) > minimum_num:
                self.enough_gathered[side] = True
            else:
                self.enough_gathered[side] = False

    @staticmethod
    def _parse_trade(msg):
        ask_price, ask_volume = int(float(msg['asks'][0][0])), float(msg['asks'][0][1])
        bid_price, bid_volume = int(float(msg['bids'][0][0])), float(msg['bids'][0][1])
        return ask_price, ask_volume, bid_price, bid_volume