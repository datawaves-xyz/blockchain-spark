import requests
import logging
from typing import Optional
from abc import ABCMeta, abstractmethod

from spark3.exceptions import FailToGetEtherscanABI


class IContractABIProvider(metaclass=ABCMeta):

    @abstractmethod
    def get_contract_abi(self, contract_address: str) -> str:
        pass


class EtherscanABIProvider(IContractABIProvider):
    logger = logging.getLogger("spark3.providers.EtherscanProvider")

    def __init__(self, apikey: Optional[str] = None):
        if apikey is None:
            self.logger.warning("API Key is not configured, EtherscanABIProvider will be rate-limited hard")
        self.apikey = apikey

    def get_contract_abi(self, contract_address: str) -> str:
        r = requests.get('https://api.etherscan.io/api', params={
            'module': 'contract',
            'action': 'getabi',
            'address': contract_address
        } | ({'apikey': self.apikey} if self.apikey else {}))
        r.raise_for_status()
        data = r.json()
        if data['status'] != '1':
            self.logger.error("Failed to get contract abi: %s:%s", data['status'], data['message'])
            raise FailToGetEtherscanABI(data['message'])
        return data['result']
