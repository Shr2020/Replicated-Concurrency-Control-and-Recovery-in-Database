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
        self.sites_affected = set()
        
        # all the variables affected (read/write)
        self.var_affected = []
        
        # queue of all operations
        self.operations = []
        
        # queueu of operation yet to be processed. 
        self.remaining_operations = []

        # type of transaction. RO: ReadOnly, RW: Read-Write
        self.type = type

        # to abortt ater
        self.abort_later = False

    def add_site(self, site):
        self.sites_affected.append(site)

    def add_operation(self, op):
        self.operations.append(op)
        self.remaining_operations.append(op)

    def get_operations(self):
        return self.operations

    def update_transaction_state(self, state):
        self.transaction_state = state

    def get_type(self):
        return self.type

    # return the next operation and pops it from the queue
    def get_op(self):
        return self.remaining_operations.pop(0)

    # return the next operation by peeking in the queue
    def read_next_op(self):
        return self.remaining_operations[0]

