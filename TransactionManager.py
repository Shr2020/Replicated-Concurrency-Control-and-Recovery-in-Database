class TransactionManager:
    def __init__(self):
        self.sites = set([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        self.vars = self.get_vars()
        self.current_transactions = {}
        self.time = 0
        self.transaction_wait_queue = []
        self.deadlocked_transaction  = []

    def tick(self):
        self.time+=1

    def begin_transaction(self, transaction_id, transaction):
        self.current_transactions[transaction_id] = transaction

    def write_operation(self, transaction_id, variable, value):
        pass

    def read_operation(self, transaction_id, variable):
        pass

    def dump(self):
        pass

    def end_transaction(self, transaction_id):
        pass

    def abort_transaction(self, transaction_id):
        pass

    def deadlock_detect(self):
        pass

    def deadlock_clear(self):
        pass
