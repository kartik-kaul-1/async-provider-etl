import asyncio
from timeit import timeit
from .__main__ import main as async_main


def main() -> None:
    elapsed_time = timeit(stmt="asyncio.run(async_main())", number=1, globals=globals())
    print(f"\nTotal elapsed time: {elapsed_time:.2f} seconds.")
