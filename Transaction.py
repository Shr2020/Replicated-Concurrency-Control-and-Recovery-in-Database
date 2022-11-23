import TransactionStates as ts

class Transaction:
    def __init__(self, id, time):
        self.transaction_id = id                                
        self.transaction_state = ""
        self.time = time
        self.sites_affected = []
        self.operations = []

    def add_site(self, site):
        self.sites_affected.append(site)

    def add_operation(self, op):
        self.operations.append(op)

    def update_transaction_state(self, state):
        self.transaction_state = state