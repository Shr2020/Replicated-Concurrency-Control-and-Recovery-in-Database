import TransactionStates as ts

class Transaction:
    def __init__(self, id, time, type):
        self.transaction_id = id                                
        self.transaction_state = ts.TransactionStates.START
        self.time = time
        self.sites_affected = []
        self.operations = []
        # pop operations as we process
        self.remaining_operations = []
        self.type = type

    def add_site(self, site):
        self.sites_affected.append(site)

    def add_operation(self, op, var, val=None):
        self.operations.append((op, var, val))
        self.remaining_operations.append((op, var, val))

    def get_operations(self):
        return self.operations

    def update_transaction_state(self, state):
        self.transaction_state = state