import requests
import logging

from spark3.exceptions import FailToGetEtherscanABI


class ContractABIProvider:
    def get_contract_abi(self, contract_address: str) -> str:
        pass


class EtherscanABIProvider(ContractABIProvider):
    logger = logging.getLogger("contract.EtherscanProvider")

    def get_contract_abi(self, contract_address):
        r = requests.get('https://api.etherscan.io/api', params={
            'module': 'contract',
            'action': 'getabi',
            'address': contract_address
        })
        r.raise_for_status()
        data = r.json()
        if data['status'] != '1':
            self.logger.error("Failed to get contract abi: %s:%s", data['status'], data['message'])
            raise FailToGetEtherscanABI()
        return data['result']
