import json
from collections import deque

import numpy as np
import pandas as pd
import websockets

from .websocket import WebSocketParent


class CoincheckWebSocketParent(WebSocketParent):
    """Coincheck WebSocket Base Class for {trade, orderbooks} APIs."""

    async def on_connect(self) -> None:
        async with websockets.connect('wss://ws-api.coincheck.com/') as ws:
            await self.subscribe(ws)
            while True:
                msg = json.loads(await ws.recv())
                await self.on_message(msg)


class CoincheckWebSocketTrade(CoincheckWebSocketParent):
    """
    Fetch Trades(Executions) Data from Coincheck WebSocket API.
    This Class calculate Bolinger bands and if the new price breaks the bands, 
    Tells it in console message.
    """

    def __init__(self, window_size: pd.Timedelta, sigma: int):
        super().__init__()
        self.order_dict = {'time': deque(), 'price': deque(), 'volume': deque(), 'side': deque()}
        self.window_size = window_size
        self.sigma = sigma
        self.upper_band = None
        self.lower_band = None
        self.enough_gathered = False

    async def subscribe(self, ws):
        msg = {'type': 'subscribe', 'channel': 'btc_jpy-trades'}
        await ws.send(json.dumps(msg))
        self.logger.info('Subscribed to trades')

    async def on_message(self, msg):
        time = pd.Timestamp.now()
        price, volume, side = self._parse_trade(msg)
        if self.enough_gathered and price > self.upper_band:
            self.logger.info(f'Break Upper Band when Price: {price} Volume {volume}')
        elif self.enough_gathered and price < self.lower_band:
            self.logger.info(f'Break Lower Band when Price: {price} Volume: {volume}')
        self._update_order_dict(time, price, volume, side)
        self.logger.info(f'{len(self.order_dict["price"])}')
        self._get_bolingers()

    def _update_order_dict(self, time, price, volume, side):
        for label, content in zip(('time', 'price', 'volume', 'side'), (time, price, volume, side)):
            self.order_dict[label].append(content)
        last = self.order_dict['time'][-1]
        while True:
            first = self.order_dict['time'][0]
            if last - first < self.window_size:
                break
            for key in self.order_dict:
                self.order_dict[key].popleft()
        self._check_order_dict_num()

    def _get_bolingers(self):
        mean = np.mean(self.order_dict['price'])
        std_dev = np.std(self.order_dict['volume'])
        self.upper_band = mean + std_dev
        self.lower_band = mean - std_dev

    def _check_order_dict_num(self, minimum_num=10):
        if len(self.order_dict['price']) > minimum_num:
            self.enough_gathered = True
        else:
            self.enough_gathered = False

    @staticmethod
    def _parse_trade(msg):
        price, volume, side = int(float(msg[2])), float(msg[3]), msg[4]
        return price, volume, side