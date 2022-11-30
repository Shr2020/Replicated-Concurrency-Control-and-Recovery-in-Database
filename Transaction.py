import TransactionStates as ts

class Transaction:
    def __init__(self, id, time, type):
        # transaction id
        self.transaction_id = id                                
        
        # state of transaction
        self.transaction_state = ts.TransactionStates.START
        
        # time the transacion began
        self.time = time
        
        # all the sites affected
        self.sites_affected = []
        
        # all the variables affected (read/write)
        self.var_affected = []
        
        # list of all operations
        self.operations = []
        
        # list of operation yet to be processed. 
        self.remaining_operations = []

        # type of transaction. RO: ReadOnly, RW: Read-Write
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

    def get_type(self):
        return self.type