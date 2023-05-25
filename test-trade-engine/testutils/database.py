from sqlalchemy import create_engine, StaticPool


def get_sqlite_engine(echo, file: str | None = None):
    assert file is None or file.startswith("/"), "file needs to start with a leading /"

    # note that we need a static pool for in memory sqlite database
    if file is None:
        return create_engine('sqlite://', echo=echo, connect_args={'check_same_thread': False}, poolclass=StaticPool)
    else:
        return create_engine(f'sqlite://{file}', echo=echo)

