import aiohttp
import asyncio
import csv
import json

from web3 import AsyncWeb3, AsyncHTTPProvider
from loguru import logger

logger.add('log.log', level='ERROR', format="{time:YYYY-MM-DD HH:mm:ss} - {message}")


rekt_token_address = AsyncWeb3.to_checksum_address('0x1d987200df3b744cfa9c14f713f5334cb4bc4d5d')
rekt_distributor_address = AsyncWeb3.to_checksum_address('0x21a2f6a0d2156bb069b3062e249072cec2da9320')


async def start(accounts_data: list):
    tasks = []
    for acc_data in accounts_data:
        try:
            user = W3User(acc_data)

            task = asyncio.create_task(user.claim_tokens())
            tasks.append(task)

        except Exception as e:
            logger.error(f"Error creating {acc_data['private_key'][:-10]}...: {e.args}")

    await asyncio.gather(*tasks)

class W3User:
    def __init__(self, account_data):
        self.private_key = account_data['private_key']
        self.http_rpc = account_data['http_rpc']
        self.referrer_address = account_data['referrer_address']
        self.max_gas_price = int(account_data['max_gas_price'])
        self.min_gas_limit = int(account_data['min_gas_limit'])

        self.w3 = AsyncWeb3(AsyncHTTPProvider(self.http_rpc))
        self.signer = self.w3.eth.account.from_key(self.private_key)
        self.rekt_distributor_contract = self.w3.eth.contract(address=rekt_distributor_address, abi=rekt_contract_abi)

        self.rect_token_contract = self.w3.eth.contract(address=rekt_token_address, abi=rekt_token_abi)

    async def _get_signature(self):
        async with aiohttp.ClientSession() as session:
            params = {"userAddress": self.signer.address}
            async with session.post('https://rektarb.xyz/api/sinature', params=params) as resp:
                signature = await resp.json()
                print(signature)
                return signature

    async def claim_tokens(self):
        nonce = await self.w3.eth.get_transaction_count(self.signer.address)
        gas_price = await self.w3.eth.gas_price * self.max_gas_price

        signature = await self._get_signature()
        if signature['signature']:
            bytes_signature = signature['signature'].encode('utf-8')

            claim_txn = await self.rekt_distributor_contract.functions.claim(int(signature['nonce']), bytes_signature, self.referrer_address).build_transaction({
                'from': self.signer.address,
                'gas': self.min_gas_limit,
                'gasPrice': gas_price,
                'nonce': nonce
            })
            signed_txn = self.signer.signTransaction(claim_txn)

            try:
                txn_hash = await self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
                txn_text = f"{self.signer.address} https://arbiscan.io/tx/{txn_hash.hex()}"

                txn_receipt = await self.w3.eth.wait_for_transaction_receipt(txn_hash)
                if txn_receipt['status']:
                    logger.info(f"Claim successful for {txn_text}")
                else:
                    logger.warning(f"Claim failed for {txn_text}")

            except Exception as e:
                logger.error(f"Error sending transaction for {self.signer.address} {str(e)}")
        else:
            logger.warning(f"{self.signer.address} Not eligible")



if __name__ == '__main__':
    with open('accs.csv', encoding='utf-8') as f:
        all_data = list(csv.DictReader(f, delimiter=','))
    #
    with open('src/ABI_distr.json', 'r') as f:
        rekt_contract_abi = json.load(f)

    with open('src/ABI_token.json', 'r') as f:
        rekt_token_abi = json.load(f)


    asyncio.run(start(all_data))
