import asyncio
from timeit import timeit
from .__main__ import main as async_main
from .util import ASCII_ART


def main() -> None:
    print(ASCII_ART)
    elapsed_time = timeit(stmt="asyncio.run(async_main())", number=1, globals=globals())
    print(f"\nTotal elapsed time: {elapsed_time:.2f} seconds.")
