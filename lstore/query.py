from __future__ import annotations
from typing import List
from lstore.table import Table
from lstore.transaction import UndoEntry


class Query:

    def __init__(self, table: Table):
        self.table = table
        self.key = table.key
        self.num_columns = table.num_columns

    def insert(self, *columns, txn=None):

        rid = self.table.insert_record(columns)

        if txn:
            txn.add_undo(
                UndoEntry(
                    typ="INSERT",
                    table=self.table,
                    base_rid=rid,
                    payload={
                        "pk": columns[self.key],
                        "row": list(columns)
                    }
                )
            )

        return True

    def delete(self, key, txn=None):

        rid = self.table.get_base_rid_by_key(key)
        if rid is None:
            return False

        old_row = self.table.read_latest_user_columns(rid)

        if txn:
            txn.add_undo(
                UndoEntry(
                    typ="DELETE",
                    table=self.table,
                    base_rid=rid,
                    payload={"old_row": old_row}
                )
            )

        return self.table.delete_record(key)

    def update(self, key, *columns, txn=None):

        rid = self.table.get_base_rid_by_key(key)
        if rid is None:
            return False

        old_row = self.table.read_latest_user_columns(rid)
        old_indirection = self.table._base_latest_tail_rid(rid)
        old_schema = self.table._base_schema(rid)

        if txn:
            txn.add_undo(
                UndoEntry(
                    typ="UPDATE",
                    table=self.table,
                    base_rid=rid,
                    payload={
                        "old_row": old_row,
                        "old_indirection": old_indirection,
                        "old_schema": old_schema
                    }
                )
            )

        return self.table.update_record(key, columns)
