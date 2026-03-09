"""
M3 Complete Tester
Covers all 10 autograder test cases:
  - M3 Normal Insert & Select Test
  - M3 Normal Update Test
  - M3 Normal Aggregate Test
  - M3 Extended Insert & Select Test
  - M3 Extended Update Test
  - M3 Extended Aggregate Test
  - M3 Extended Version 0 Select Test
  - M3 Correctness Test 1
  - M3 Correctness Test 2
  - M3 2PL Test

Run from the project root (the folder that contains the lstore/ package):
    python m3_tester.py
"""

import shutil
import os
import sys
import traceback
from random import randint, seed

from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

# ── terminal colours ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

passed = []
failed = []

DB_PATH = "./m3_test_scratch"


def _clean():
    shutil.rmtree(DB_PATH, ignore_errors=True)


def run_test(name, fn):
    _clean()
    try:
        fn()
        print(f"  {GREEN}PASS{RESET}  {name}")
        passed.append(name)
    except AssertionError as e:
        print(f"  {RED}FAIL{RESET}  {name}")
        print(f"         AssertionError: {e}")
        failed.append(name)
    except Exception as e:
        print(f"  {RED}FAIL{RESET}  {name}")
        traceback.print_exc()
        failed.append(name)
    finally:
        _clean()


# ─────────────────────────────────────────────────────────────────────────────
# Helper: distribute transactions across N worker threads and wait for all
# ─────────────────────────────────────────────────────────────────────────────
def _run_transactions(transactions, num_threads=8):
    workers = [TransactionWorker() for _ in range(num_threads)]
    for i, t in enumerate(transactions):
        workers[i % num_threads].add_transaction(t)
    for w in workers:
        w.run()
    for w in workers:
        w.join()
    return workers


# =============================================================================
# 1.  M3 Normal Insert & Select Test
#     Insert 100 records via transactions, verify every record reads back
#     correctly in the main thread.
# =============================================================================
def test_normal_insert_select():
    db = Database()
    db.open(DB_PATH)
    table = db.create_table("Grades", 5, 0)
    query = Query(table)

    seed(1001)
    records = {}
    transactions = []

    for i in range(100):
        key = 10000 + i
        row = [key, randint(0, 100), randint(0, 100), randint(0, 100), randint(0, 100)]
        records[key] = row
        t = Transaction()
        t.add_query(query.insert, table, *row)
        transactions.append(t)

    _run_transactions(transactions)

    for key, expected in records.items():
        result = query.select(key, 0, [1, 1, 1, 1, 1])
        assert result and len(result) == 1, f"No record found for key {key}"
        assert result[0].columns == expected, \
            f"Key {key}: expected {expected}, got {result[0].columns}"

    db.close()


# =============================================================================
# 2.  M3 Normal Update Test
#     Insert 100 records, update columns 2 and 3, verify final state.
# =============================================================================
def test_normal_update():
    db = Database()
    db.open(DB_PATH)
    table = db.create_table("Grades", 5, 0)
    query = Query(table)

    seed(1002)
    records = {}
    insert_txns = []

    for i in range(100):
        key = 20000 + i
        row = [key, randint(0, 50), randint(0, 50), randint(0, 50), randint(0, 50)]
        records[key] = row[:]
        t = Transaction()
        t.add_query(query.insert, table, *row)
        insert_txns.append(t)

    _run_transactions(insert_txns)

    update_txns = []
    for key in records:
        new_c2 = randint(100, 200)
        new_c3 = randint(100, 200)
        records[key][2] = new_c2
        records[key][3] = new_c3
        t = Transaction()
        t.add_query(query.update, table, key, None, None, new_c2, new_c3, None)
        update_txns.append(t)

    _run_transactions(update_txns)

    for key, expected in records.items():
        result = query.select(key, 0, [1, 1, 1, 1, 1])
        assert result and len(result) == 1, f"No record found for key {key}"
        assert result[0].columns == expected, \
            f"Key {key}: expected {expected}, got {result[0].columns}"

    db.close()


# =============================================================================
# 3.  M3 Normal Aggregate Test
#     Insert records with known values, verify sum over key ranges.
# =============================================================================
def test_normal_aggregate():
    db = Database()
    db.open(DB_PATH)
    table = db.create_table("Grades", 5, 0)
    query = Query(table)

    insert_txns = []
    for i in range(1, 21):
        t = Transaction()
        t.add_query(query.insert, table, i, i * 10, i * 20, i * 30, i * 40)
        insert_txns.append(t)
    _run_transactions(insert_txns)

    # sum col1 for keys 1..10 = 10+20+...+100 = 550
    result = query.sum(1, 10, 1)
    expected = sum(i * 10 for i in range(1, 11))
    assert result == expected, f"sum(1,10,col1): expected {expected}, got {result}"

    # sum col2 for keys 5..15
    result2 = query.sum(5, 15, 2)
    expected2 = sum(i * 20 for i in range(5, 16))
    assert result2 == expected2, f"sum(5,15,col2): expected {expected2}, got {result2}"

    db.close()


# =============================================================================
# 4.  M3 Extended Insert & Select Test
#     1000 records, 100 transactions, 8 threads, secondary indexes on cols 2-4.
#     Uses the exact same seed as the provided reference testers.
# =============================================================================
def test_extended_insert_select():
    db = Database()
    db.open(DB_PATH)
    table = db.create_table("Grades", 5, 0)
    query = Query(table)

    try:
        table.index.create_index(2)
        table.index.create_index(3)
        table.index.create_index(4)
    except Exception:
        pass

    number_of_records = 1000
    number_of_transactions = 100
    num_threads = 8
    seed(3562901)

    keys = []
    records = {}
    transactions = [Transaction() for _ in range(number_of_transactions)]

    for i in range(number_of_records):
        key = 92106429 + i
        keys.append(key)
        row = [key,
               randint(i * 20, (i + 1) * 20),
               randint(i * 20, (i + 1) * 20),
               randint(i * 20, (i + 1) * 20),
               randint(i * 20, (i + 1) * 20)]
        records[key] = row
        transactions[i % number_of_transactions].add_query(query.insert, table, *row)

    _run_transactions(transactions, num_threads)

    errors = 0
    for key in keys:
        result = query.select(key, 0, [1, 1, 1, 1, 1])
        if not result or len(result) == 0:
            errors += 1
            continue
        if result[0].columns != records[key]:
            errors += 1

    assert errors == 0, \
        f"Extended insert/select: {errors}/{number_of_records} records mismatched"

    db.close()


# =============================================================================
# 5.  M3 Extended Update Test
#     Mirrors the two-part reference tester exactly:
#     part 1 — insert 1000 records, 8 threads
#     part 2 — 10 rounds of updates, 8 threads, verify final values
# =============================================================================
def test_extended_update():
    db = Database()
    db.open(DB_PATH)
    table = db.create_table("Grades", 5, 0)
    query = Query(table)

    try:
        table.index.create_index(2)
        table.index.create_index(3)
        table.index.create_index(4)
    except Exception:
        pass

    number_of_records = 1000
    number_of_transactions = 100
    number_of_operations_per_record = 10
    num_threads = 8
    seed(3562901)

    keys = []
    records = {}
    insert_transactions = [Transaction() for _ in range(number_of_transactions)]

    for i in range(number_of_records):
        key = 92106429 + i
        keys.append(key)
        row = [key,
               randint(i * 20, (i + 1) * 20),
               randint(i * 20, (i + 1) * 20),
               randint(i * 20, (i + 1) * 20),
               randint(i * 20, (i + 1) * 20)]
        records[key] = row
        insert_transactions[i % number_of_transactions].add_query(
            query.insert, table, *row)

    _run_transactions(insert_transactions, num_threads)

    # Update phase — same logic as m3_tester_part_2.py
    update_transactions = [Transaction() for _ in range(number_of_transactions)]

    for j in range(number_of_operations_per_record):
        for key in keys:
            updated_columns = [None, None, None, None, None]
            for i in range(2, table.num_columns):
                value = randint(0, 20)
                updated_columns[i] = value
                records[key][i] = value
            update_transactions[key % number_of_transactions].add_query(
                query.update, table, key, *updated_columns)

    _run_transactions(update_transactions, num_threads)

    score = 0
    for key in keys:
        try:
            result = query.select(key, 0, [1, 1, 1, 1, 1])
            if result and result[0].columns == records[key]:
                score += 1
        except Exception:
            pass

    assert score == number_of_records, \
        f"Extended update: {score}/{number_of_records} records correct"

    db.close()


# =============================================================================
# 6.  M3 Extended Aggregate Test
#     Insert 200 records, update col1 for all, verify sum over ranges.
# =============================================================================
def test_extended_aggregate():
    db = Database()
    db.open(DB_PATH)
    table = db.create_table("Grades", 5, 0)
    query = Query(table)

    seed(4242)
    number_of_records = 200
    records = {}
    insert_txns = [Transaction() for _ in range(20)]

    for i in range(number_of_records):
        key = 1000 + i
        row = [key, randint(1, 100), randint(1, 100), randint(1, 100), randint(1, 100)]
        records[key] = row[:]
        insert_txns[i % 20].add_query(query.insert, table, *row)

    _run_transactions(insert_txns)

    # Update col1 for all records
    update_txns = [Transaction() for _ in range(20)]
    keys_list = list(records.keys())
    for i, key in enumerate(keys_list):
        new_val = randint(200, 300)
        records[key][1] = new_val
        update_txns[i % 20].add_query(
            query.update, table, key, None, new_val, None, None, None)
    _run_transactions(update_txns)

    # Verify sum over col1 for keys 1000..1099
    expected = sum(records[k][1] for k in range(1000, 1100) if k in records)
    result = query.sum(1000, 1099, 1)
    assert result == expected, \
        f"Extended aggregate col1(1000-1099): expected {expected}, got {result}"

    # Verify sum over col2 for keys 1050..1149
    expected2 = sum(records[k][2] for k in range(1050, 1150) if k in records)
    result2 = query.sum(1050, 1149, 2)
    assert result2 == expected2, \
        f"Extended aggregate col2(1050-1149): expected {expected2}, got {result2}"

    db.close()


# =============================================================================
# 7.  M3 Extended Version 0 Select Test
#     Insert records, update them, then verify:
#       select_version(..., 0)  → latest values
#       select_version(..., -1) → pre-update (original insert) values
# =============================================================================
def test_extended_version_select():
    db = Database()
    db.open(DB_PATH)
    table = db.create_table("Grades", 5, 0)
    query = Query(table)

    seed(7777)
    number_of_records = 200
    keys = list(range(5000, 5000 + number_of_records))
    original = {}
    insert_txns = [Transaction() for _ in range(20)]

    for i, key in enumerate(keys):
        row = [key, randint(1, 50), randint(1, 50), randint(1, 50), randint(1, 50)]
        original[key] = row[:]
        insert_txns[i % 20].add_query(query.insert, table, *row)
    _run_transactions(insert_txns)

    # Update col1 for every record
    updated = {k: v[:] for k, v in original.items()}
    update_txns = [Transaction() for _ in range(20)]
    for i, key in enumerate(keys):
        new_val = randint(100, 200)
        updated[key][1] = new_val
        update_txns[i % 20].add_query(
            query.update, table, key, None, new_val, None, None, None)
    _run_transactions(update_txns)

    # Version 0 = latest
    errors_v0 = 0
    for key in keys:
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)
        if not result:
            errors_v0 += 1
            continue
        if result[0].columns != updated[key]:
            errors_v0 += 1

    assert errors_v0 == 0, \
        f"Version 0 select: {errors_v0}/{number_of_records} records wrong " \
        f"(version 0 should match latest)"

    # Version -1 = one step back = original insert
    errors_v1 = 0
    for key in keys:
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)
        if not result:
            errors_v1 += 1
            continue
        if result[0].columns != original[key]:
            errors_v1 += 1

    assert errors_v1 == 0, \
        f"Version -1 select: {errors_v1}/{number_of_records} records wrong " \
        f"(version -1 should match pre-update values)"

    db.close()


# =============================================================================
# 8.  M3 Correctness Test 1
#     Exact replica of m3_tester_part_1.py: 1000 records, 100 transactions,
#     8 threads, seed 3562901, keys starting at 92106429.
# =============================================================================
def test_correctness_1():
    db = Database()
    db.open(DB_PATH)
    grades_table = db.create_table("Grades", 5, 0)
    query = Query(grades_table)

    try:
        grades_table.index.create_index(2)
        grades_table.index.create_index(3)
        grades_table.index.create_index(4)
    except Exception:
        pass

    number_of_records = 1000
    number_of_transactions = 100
    num_threads = 8
    seed(3562901)

    keys = []
    records = {}
    insert_transactions = [Transaction() for _ in range(number_of_transactions)]

    for i in range(number_of_records):
        key = 92106429 + i
        keys.append(key)
        records[key] = [key,
                        randint(i * 20, (i + 1) * 20),
                        randint(i * 20, (i + 1) * 20),
                        randint(i * 20, (i + 1) * 20),
                        randint(i * 20, (i + 1) * 20)]
        insert_transactions[i % number_of_transactions].add_query(
            query.insert, grades_table, *records[key])

    workers = [TransactionWorker() for _ in range(num_threads)]
    for i in range(number_of_transactions):
        workers[i % num_threads].add_transaction(insert_transactions[i])
    for w in workers:
        w.run()
    for w in workers:
        w.join()

    errors = 0
    for key in keys:
        result = query.select(key, 0, [1, 1, 1, 1, 1])
        if not result or len(result) == 0:
            errors += 1
            continue
        if result[0].columns != records[key]:
            errors += 1

    assert errors == 0, f"Correctness Test 1: {errors}/{number_of_records} records mismatched"
    db.close()


# =============================================================================
# 9.  M3 Correctness Test 2
#     Simulates the two-script handoff: insert → close → re-open → update →
#     verify. Uses the same seed and parameters as both reference testers.
# =============================================================================
def test_correctness_2():
    # ---- Part 1: insert (mirrors m3_tester_part_1.py) ----------------------
    db = Database()
    db.open(DB_PATH)
    grades_table = db.create_table("Grades", 5, 0)
    query = Query(grades_table)

    try:
        grades_table.index.create_index(2)
        grades_table.index.create_index(3)
        grades_table.index.create_index(4)
    except Exception:
        pass

    number_of_records = 1000
    number_of_transactions = 100
    number_of_operations_per_record = 10
    num_threads = 8
    seed(3562901)

    keys = []
    records = {}
    insert_transactions = [Transaction() for _ in range(number_of_transactions)]

    for i in range(number_of_records):
        key = 92106429 + i
        keys.append(key)
        records[key] = [key,
                        randint(i * 20, (i + 1) * 20),
                        randint(i * 20, (i + 1) * 20),
                        randint(i * 20, (i + 1) * 20),
                        randint(i * 20, (i + 1) * 20)]
        insert_transactions[i % number_of_transactions].add_query(
            query.insert, grades_table, *records[key])

    workers = [TransactionWorker() for _ in range(num_threads)]
    for i in range(number_of_transactions):
        workers[i % num_threads].add_transaction(insert_transactions[i])
    for w in workers:
        w.run()
    for w in workers:
        w.join()

    db.close()  # flush to disk

    # ---- Part 2: re-open, update, verify (mirrors m3_tester_part_2.py) -----
    db2 = Database()
    db2.open(DB_PATH)
    grades_table2 = db2.get_table("Grades")
    assert grades_table2 is not None, \
        "get_table('Grades') returned None after db.close() + db.open()"
    query2 = Query(grades_table2)

    # Re-generate the same records dict with the same seed
    keys2 = []
    records2 = {}
    seed(3562901)
    for i in range(number_of_records):
        key = 92106429 + i
        keys2.append(key)
        records2[key] = [key,
                         randint(i * 20, (i + 1) * 20),
                         randint(i * 20, (i + 1) * 20),
                         randint(i * 20, (i + 1) * 20),
                         randint(i * 20, (i + 1) * 20)]

    update_transactions = [Transaction() for _ in range(number_of_transactions)]

    for j in range(number_of_operations_per_record):
        for key in keys2:
            updated_columns = [None, None, None, None, None]
            for i in range(2, grades_table2.num_columns):
                value = randint(0, 20)
                updated_columns[i] = value
                records2[key][i] = value
            update_transactions[key % number_of_transactions].add_query(
                query2.update, grades_table2, key, *updated_columns)

    workers2 = [TransactionWorker() for _ in range(num_threads)]
    for i in range(number_of_transactions):
        workers2[i % num_threads].add_transaction(update_transactions[i])
    for w in workers2:
        w.run()
    for w in workers2:
        w.join()

    score = 0
    for key in keys2:
        try:
            result = query2.select(key, 0, [1, 1, 1, 1, 1])
            if result and result[0].columns == records2[key]:
                score += 1
        except Exception:
            pass

    assert score == number_of_records, \
        f"Correctness Test 2: {score}/{number_of_records} correct after re-open + update"

    db2.close()


# =============================================================================
# 10. M3 2PL Test
#     Verifies that:
#     (a) Concurrent increment transactions on a shared record produce no lost
#         updates — final value equals the number of transactions.
#     (b) All transactions eventually commit (workers retry after abort).
#     (c) Two independent sets of write transactions on non-overlapping keys
#         do not interfere with each other.
# =============================================================================
def test_2pl():
    db = Database()
    db.open(DB_PATH)
    table = db.create_table("Grades", 5, 0)
    query = Query(table)

    # ---- (a) + (b): concurrent increments on a shared record ----------------
    shared_key = 99999
    t_init = Transaction()
    t_init.add_query(query.insert, table, shared_key, 0, 0, 0, 0)
    assert t_init.run(), "Initial insert for shared key failed"

    num_increment_txns = 50
    num_threads = 8
    increment_txns = []
    for _ in range(num_increment_txns):
        t = Transaction()
        t.add_query(query.select, table, shared_key, 0, [1, 1, 1, 1, 1])
        t.add_query(query.increment, table, shared_key, 1)
        increment_txns.append(t)

    workers = _run_transactions(increment_txns, num_threads)

    total_commits = sum(w._commits for w in workers)
    assert total_commits == num_increment_txns, \
        f"2PL: expected {num_increment_txns} total commits, got {total_commits}"

    result = query.select(shared_key, 0, [1, 1, 1, 1, 1])
    assert result and len(result) == 1, "Shared record not found after concurrent increments"
    final_val = result[0].columns[1]
    assert final_val == num_increment_txns, \
        f"2PL lost-update: expected col1={num_increment_txns}, got {final_val}"

    # ---- (c): two independent write groups don't interfere ------------------
    keys_a = list(range(10000, 10020))
    keys_b = list(range(10020, 10040))

    setup_txns = []
    for k in keys_a + keys_b:
        t = Transaction()
        t.add_query(query.insert, table, k, k, 0, 0, 0)
        setup_txns.append(t)
    _run_transactions(setup_txns, num_threads)

    txns_a = []
    for k in keys_a:
        t = Transaction()
        t.add_query(query.update, table, k, None, 1, None, None, None)
        txns_a.append(t)

    txns_b = []
    for k in keys_b:
        t = Transaction()
        t.add_query(query.update, table, k, None, 2, None, None, None)
        txns_b.append(t)

    # Run both groups concurrently
    all_workers = [TransactionWorker() for _ in range(8)]
    for i, t in enumerate(txns_a):
        all_workers[i % 4].add_transaction(t)
    for i, t in enumerate(txns_b):
        all_workers[4 + i % 4].add_transaction(t)
    for w in all_workers:
        w.run()
    for w in all_workers:
        w.join()

    for k in keys_a:
        r = query.select(k, 0, [1, 1, 1, 1, 1])
        assert r and r[0].columns[1] == 1, \
            f"2PL isolation: key {k} col1 should be 1, got {r[0].columns[1] if r else 'None'}"
    for k in keys_b:
        r = query.select(k, 0, [1, 1, 1, 1, 1])
        assert r and r[0].columns[1] == 2, \
            f"2PL isolation: key {k} col1 should be 2, got {r[0].columns[1] if r else 'None'}"

    db.close()


# =============================================================================
# Main
# =============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 62)
    print("  M3 Full Test Suite")
    print("=" * 62)

    run_test("M3 Normal Insert & Select Test",    test_normal_insert_select)
    run_test("M3 Normal Update Test",             test_normal_update)
    run_test("M3 Normal Aggregate Test",          test_normal_aggregate)
    run_test("M3 Extended Insert & Select Test",  test_extended_insert_select)
    run_test("M3 Extended Update Test",           test_extended_update)
    run_test("M3 Extended Aggregate Test",        test_extended_aggregate)
    run_test("M3 Extended Version 0 Select Test", test_extended_version_select)
    run_test("M3 Correctness Test 1",             test_correctness_1)
    run_test("M3 Correctness Test 2",             test_correctness_2)
    run_test("M3 2PL Test",                       test_2pl)

    print("\n" + "=" * 62)
    print(f"  Results:  {GREEN}{len(passed)} passed{RESET}  /  "
          f"{RED}{len(failed)} failed{RESET}  out of {len(passed)+len(failed)}")
    if failed:
        print(f"\n  {YELLOW}Failed:{RESET}")
        for name in failed:
            print(f"    - {name}")
    print("=" * 62 + "\n")

    sys.exit(0 if not failed else 1)
