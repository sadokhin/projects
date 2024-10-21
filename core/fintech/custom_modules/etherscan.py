import logging
import os
import cloudscraper
import requests
from typing import Union
import json

logger = logging.getLogger(__name__)

class Etherscan:
    def __init__(self, etherscan_api_key):
        self.etherscan_api_key = etherscan_api_key
        self.base_url = "https://api.etherscan.io/api"
        self.scraper = cloudscraper.CloudScraper()

    def _ether_get_params(self, module, action, **kwargs):
        return {'module': module, 'action': action, 'apikey': self.etherscan_api_key, **kwargs}

    def _ether_query(self, net, params) -> requests.Response:
        url = self.base_url if net == 'mainnet' else f"https://api-{net}.etherscan.io/api"
        response = self.scraper.get(url, params=params)
        return response

    def _ether_get_result(self, response: requests.Response):
        return response.json().get('result')

    def get_tx_by_hash(self, txhash, net='mainnet'):
        params = self._ether_get_params(module='proxy', action='eth_getTransactionByHash', txhash=txhash)
        response = self._ether_query(net, params)
        return self._ether_get_result(response)

    def get_tx_receipt(self, txhash, net='mainnet'):
        params = self._ether_get_params(module='proxy', action='eth_getTransactionReceipt', txhash=txhash)
        response = self._ether_query(net, params)
        return self._ether_get_result(response)

    def get_acc_txs(self, acc, net='mainnet'):
        params = self._ether_get_params(module='account', action='txlist', sort='desc', address=acc)
        response = self._ether_query(net, params)
        return self._ether_get_result(response)

    def get_tx_internal_txs(self, txhash, net='mainnet'):
        params = self._ether_get_params(module='account', action='txlistinternal', sort='desc', txhash=txhash)
        response = self._ether_query(net, params)
        return self._ether_get_result(response)

    def get_acc_erc_txs(self, acc, net='mainnet'):
        params = self._ether_get_params(module='account', action='tokentx', sort='desc', address=acc)
        response = self._ether_query(net, params)
        return self._ether_get_result(response)

    def get_logs(self, topic, address, net='mainnet'):
        params = self._ether_get_params(module='logs', action='getLogs', sort='desc', topic0=topic, address=address)
        response = self._ether_query(net, params)
        return self._ether_get_result(response)

    def get_block_by_number(self, block: Union[int, str], boolean=False, net='mainnet'):
        try:
            tag = hex(block)
        except TypeError:
            tag = block
        params = self._ether_get_params(module='proxy', action='eth_getBlockByNumber', tag=tag, boolean=boolean)
        response = self._ether_query(net, params)
        result = self._ether_get_result(response)
        return result



    def get_contracts(self, address, net='mainnet'):
        params = self.ether_get_params(module='contract', action='getsourcecode', apikey=self.etherscan_api_key, address=address)
        result = self.ether_get_result(self.ether_query(net, params))[0]
        source_code_raw: str = result['SourceCode']
        if source_code_raw.startswith('{{') or source_code_raw.startswith('{'):
            sources = json.loads(source_code_raw[1:-1])['sources'] if source_code_raw.startswith('{{') else json.loads(source_code_raw)
            contents = {k: v['content'] for k, v in sources.items()}
        else:
            contents = {f"{result['ContractName']}.sol": source_code_raw}
        return result['ContractName'], contents

    def get_common_root(self, contents):
        roots = {os.path.commonpath([k]) for k in contents.keys()}
        root = roots.pop() if len(roots) == 1 else ''
        return root

    def write_contents(self, contents, destination_dir, strip_common_dir=True):
        common_root = self.get_common_root(contents) if strip_common_dir else ''
        for rel_path_raw, code in contents.items():
            rel_path = os.path.relpath(rel_path_raw, common_root) if common_root else rel_path_raw
            path = os.path.join(destination_dir, rel_path)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w') as fi:
                fi.write(code)
    
    def ether_get_gas_info(self, tx_hash):
        try:
            info = self.get_tx_receipt(tx_hash)
            return int(info["effectiveGasPrice"], 16), int(info["gasUsed"], 16)
        except Exception as e:
            logger.info(f'error: {e}, info better 0 for tx_hash: {tx_hash}')
            return 0, 0