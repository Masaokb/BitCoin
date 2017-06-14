import asyncio

from ws import *

window_size = pd.Timedelta('10 min')
sigma = 1
task = [CoincheckWebSocketTrade(window_size=window_size, sigma=sigma).on_connect(),
        ZaifWebSocketBTCJPY(window_size=window_size, sigma=sigma).on_connect()]
loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(task))
