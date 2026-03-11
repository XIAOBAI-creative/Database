from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple, Dict
from contextlib import nullcontext
import threading

from lstore.table import Table
from lstore.lock_manager import LockManager, LockConflict


@dataclass
class UndoEntry:
    typ: str
    table: Table
    base_rid: int
    payload: Dict[str, Any]


_TXN_ID_LOCK = threading.Lock()
_TXN_ID = 1


def _next_txn_id():
    global _TXN_ID
    with _TXN_ID_LOCK:
        tid = _TXN_ID
        _TXN_ID += 1
        return tid


class Transaction:

    def __init__(self):
        self.queries: List[Tuple[Callable[..., Any], Table, Tuple[Any, ...]]] = []
        self.txn_id = _next_txn_id()
        self._undo: List[UndoEntry] = []
        self._last_abort_reason: Optional[str] = None

    def add_query(self, query: Callable[..., Any], table: Table, *args):
        if not hasattr(table, "lock_manager") or table.lock_manager is None:
            table.lock_manager = LockManager()

        self.queries.append((query, table, args))

    def _release_all_locks(self):
        released = set()
        for (_, table, _) in self.queries:
            lm = getattr(table, "lock_manager", None)
            if lm and id(lm) not in released:
                lm.release_all(self.txn_id)
                released.add(id(lm))

    def abort(self):

        for i in range(len(self._undo) - 1, -1, -1):
            undo = self._undo[i]
            self._apply_undo(undo)

        self._release_all_locks()
        return False

    def commit(self):
        self._release_all_locks()
        return True

    def run(self):

        try:
            for query, table, args in self.queries:

                result = query(*args, txn=self)

                if result is False or result is None:
                    self._last_abort_reason = "QUERY_FAIL"
                    return self.abort()

            return self.commit()

        except LockConflict:
            self._last_abort_reason = "LOCK"
            return self.abort()

        except Exception as e:
            self._last_abort_reason = "EXCEPTION"
            return self.abort()

    def add_undo(self, undo: UndoEntry):
        self._undo.append(undo)

    def _apply_undo(self, undo: UndoEntry):

        table = undo.table

        if undo.typ == "INSERT":

            rid = undo.base_rid
            pk = undo.payload["pk"]

            with table._meta_lock:
                table._deleted.pop(rid, None)
                table._latest_cache.pop(rid, None)
                table.key2rid.pop(pk, None)
                table.page_directory.pop(rid, None)

            for c in range(table.num_columns):
                if table.index.is_indexed(c):
                    val = undo.payload["row"][c]
                    table.index.delete_entry(c, val, rid)

        elif undo.typ == "DELETE":

            rid = undo.base_rid
            row = undo.payload["old_row"]

            with table._meta_lock:
                table._deleted[rid] = False
                table._latest_cache[rid] = row
                table.key2rid[row[table.key]] = rid

            for c in range(table.num_columns):
                if table.index.is_indexed(c):
                    table.index.insert_entry(c, row[c], rid)

        elif undo.typ == "UPDATE":

            rid = undo.base_rid
            row = undo.payload["old_row"]

            table.overwrite_base_indirection(rid, undo.payload["old_indirection"])
            table.overwrite_base_schema(rid, undo.payload["old_schema"])

            with table._meta_lock:
                table._latest_cache[rid] = row

            new_row = table.read_latest_user_columns(rid)

            for c in range(table.num_columns):
                if table.index.is_indexed(c):

                    old_v = row[c]
                    new_v = new_row[c]

                    if old_v != new_v:
                        table.index.delete_entry(c, new_v, rid)
                        table.index.insert_entry(c, old_v, rid)
