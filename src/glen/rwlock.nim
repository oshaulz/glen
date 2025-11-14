import std/locks

type
  RwLock* = object
    m: Lock
    readers: int
    writer: bool
    readersCond: Cond
    writersCond: Cond
    writersWaiting: int

proc initRwLock*(rw: var RwLock) =
  initLock(rw.m)
  initCond(rw.readersCond)
  initCond(rw.writersCond)
  rw.readers = 0
  rw.writer = false
  rw.writersWaiting = 0

proc acquireRead*(rw: var RwLock) =
  acquire(rw.m)
  # Reader-prefer by default; if compiled with -d:rwlockFair, block new readers when writers are waiting
  when defined(rwlockFair):
    while rw.writer or rw.writersWaiting > 0:
      wait(rw.readersCond, rw.m)
  else:
    # allow readers unless a writer is active
    while rw.writer:
      wait(rw.readersCond, rw.m)
  inc rw.readers
  release(rw.m)

proc releaseRead*(rw: var RwLock) =
  acquire(rw.m)
  dec rw.readers
  if rw.readers == 0:
    signal(rw.writersCond)
  release(rw.m)

proc acquireWrite*(rw: var RwLock) =
  acquire(rw.m)
  inc rw.writersWaiting
  while rw.writer or rw.readers > 0:
    wait(rw.writersCond, rw.m)
  dec rw.writersWaiting
  rw.writer = true
  release(rw.m)

proc releaseWrite*(rw: var RwLock) =
  acquire(rw.m)
  rw.writer = false
  if rw.writersWaiting > 0:
    signal(rw.writersCond)
  else:
    broadcast(rw.readersCond)
  release(rw.m)


