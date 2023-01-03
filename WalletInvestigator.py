import time
import json
import asyncio
import aiohttp
from web3 import Web3


class WalletInvestigator:

    def __init__(self, addr, max_depth):
        self.acc = addr
        self.accounts = []
        self.accounts_to_check = []
        self.accounts_checked = []
        self.transfers = []
        self.key = 'API KEY'
        self.max_depth = max_depth
        self.tasks = []

    async def run(self):
        await self.get_transactions(self.acc)
        self.accounts_to_check = self.accounts.copy()
        self.accounts_checked.extend(self.accounts)
        self.accounts.clear()
        total = 0
        for i in range(self.max_depth - 1):
            print('depth = ' + str(i))
            while self.accounts_to_check:
                total += 1
                st_t = time.perf_counter()
                a = self.accounts_to_check.pop()
                self.tasks.append(asyncio.create_task(self.get_transactions(a)))
                wait = 1 - (time.perf_counter() - st_t)
                await asyncio.sleep(wait if wait > 0 else 0)
                await asyncio.sleep(0.25)
                if total % 5 == 0:
                    print(total)
            for j, task in enumerate(self.tasks):
                print('task: ' + str(j))
                await task
            self.tasks.clear()
            self.accounts_to_check = self.accounts.copy()
            self.accounts_checked.extend(self.accounts)
            self.accounts.clear()
        with open(self.acc + '_tx-data-raw.txt', 'w') as outfile:
            outfile.write(json.dumps(self.transfers, indent=4))

    async def get_transactions(self, acc):
        try:
            async with aiohttp.ClientSession() as session:
                url = 'https://api.covalenthq.com/v1/56/address/' + acc + '/transactions_v2/?key=' + self.key
                async with session.get(url) as r:
                    r.raise_for_status()
                    self.add_transfers((await r.json())['data']['items'])
        except Exception as err:
            print(err)

    def add_transfers(self, txs):
        for tx in txs:
            if tx['value'] == '0':
                continue
            if tx['to_address'] not in self.accounts and tx['to_address'] not in self.accounts_checked and \
               tx['to_address'] is not None:
                self.accounts.append(tx['to_address'])
            self.transfers.append(tx)

    def clean_data(self):
        raw = open(self.acc + '_tx-data-raw.txt')
        raw_json = json.load(raw)
        raw.close()
        cleaned = {}
        for tx in raw_json:
            fr = tx['from_address'] if tx['from_address_label'] is None else tx['from_address_label']
            to = tx['to_address'] if tx['to_address_label'] is None else tx['to_address_label']
            va = tx['value']
            try:
                cleaned[fr][to] += int(va)
            except KeyError:
                cleaned[fr] = {
                    to: int(va)
                }
        cleaned_f = open(self.acc + '_tx-data-clean.txt', 'w')
        cleaned_f.write('source,target,weight\n')
        for f in cleaned:
            for t in cleaned[f]:
                if t is None or t == '':
                    continue
                if f is None or f == '':
                    continue
                cleaned_f.write(','.join([f, t, str(cleaned[f][t] / pow(10, 18))]))
                cleaned_f.write('\n')
        cleaned_f.close()
