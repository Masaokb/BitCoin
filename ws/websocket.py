
from abc import ABCMeta, abstractmethod

from .utils import make_logger


class WebSocketParent(metaclass=ABCMeta):
    """
    WebSocket Base Class to receive some exchanges which provide WebSocekt API.
    i.e.) Coincheck, Zaif
    """

    def __init__(self):
        self.logger = make_logger()
        self.logger.info('Initializing...')

    @abstractmethod
    async def on_connect(self) -> None:
        """Connect to WebSocket API"""

    @abstractmethod
    async def subscribe(self, ws) -> None:
        """Send json to start to subscribe"""

    @abstractmethod
    async def on_message(self, msg) -> None:
        """Processes of callback on message receiving."""
