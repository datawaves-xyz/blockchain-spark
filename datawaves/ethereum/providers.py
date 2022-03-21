import logging
import requests

from spark3.providers import IContractABIProvider

class DatawavesABIProvider(IContractABIProvider):
    logger = logging.getLogger("datawaves.DatawavesProvider")

    def get_contract_abi(self, contract_address: str) -> str:
        r = requests.get("http://abi-cache.tellery-prod:3000/api/getabi", params={
            'address': contract_address
        })
        r.raise_for_status()
        return r.text