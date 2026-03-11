import threading
import random
import time


class TransactionWorker:

    def __init__(self, transactions=None):
        self.transactions = transactions or []
        self.stats = []
        self.result = 0
        self.thread = None

    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()

    def join(self):
        if self.thread:
            self.thread.join()

    def __run(self):

        for txn in self.transactions:

            while True:

                result = txn.run()

                if result:
                    self.stats.append(True)
                    break

                if txn._last_abort_reason != "LOCK":
                    break

                time.sleep(random.uniform(0.001, 0.05))

        self.result = sum(self.stats)
