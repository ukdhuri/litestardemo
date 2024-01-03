from litestar.stores.redis import RedisStore
from litestar.stores.memory import MemoryStore
from litestar.stores.file import FileStore
from pathlib import Path

memorystore = MemoryStore()
root_store = RedisStore.with_client()
cache_store = root_store.with_namespace("cache")
session_store = root_store.with_namespace("sessions")
tempdatard_store = root_store.with_namespace("tempdatard")
tempdatafs_store  = MemoryStore()
#tempdatafs_store=FileStore(Path("tempdatafs"))
response_cache_store = MemoryStore()
#response_cache_store = FileStore(Path("response-cache"))

# memorystore.set("foo", b"barm", expires_in=600)
# tempdatard_store.set("foo", b"bard", expires_in=600)
# tempdatafs_store.set("foo", b"barf", expires_in=600)





stores={"memory": memorystore,"root": root_store,"cache": cache_store,"sessions": session_store, "response_cache": response_cache_store,"tempdatard": tempdatard_store,"tempdatafs": tempdatafs_store}


