from WalletInvestigator import WalletInvestigator
import asyncio


async def main():
    wi = WalletInvestigator(
        addr='0x7f7c5b93ec644cdd4faece3760736fae02dc5b7a',
        max_depth=8
    )
    await wi.run()
    wi.clean_data()


if __name__ == '__main__':
    asyncio.run(main())
