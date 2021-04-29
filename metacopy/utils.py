from contextlib import asynccontextmanager


@asynccontextmanager
async def maybe_atomic(connection, atomic):
    if atomic:
        async with connection.transaction():
            yield
    else:
        yield
