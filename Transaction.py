from enum import Enum

class TransactionStates(Enum):
    BLOCK = 0                     # transaction couldn’t proceed because of chance of deadlock
    RUNNING  = 1                  # transaction has needed locks and its running
    ABORTED  = 2                  # aborted 
    COMMITED = 3                  # transaction completed
    WAITING  = 4                  # waiting transaction for a lock
    START = 5                     # transaction started
    TO_BE_ABORTED = 6             # transaction to be aborted due to site failure in the middle of transaction

class TransactionType(Enum):
    READ_ONLY = 0                 # READ ONLY Transaction
    READ_WRITE = 1                # READ-WRITE Transaction

class Transaction:
    def __init__(self, id, time, type):
        # transaction id
        self.transaction_id = id                                
        
        # state of transaction
        self.transaction_state = TransactionStates.START
        
        # time the transacion began
        self.time = time
        
        # all the sites affected
        self.sites_affected = set()
        
        # all the variables affected (read/write)
        self.var_affected = set()
        
        # queue of all operations
        self.operations = []
        
        # queueu of operation yet to be processed. 
        self.remaining_operations = []

        # type of transaction. RO: ReadOnly, RW: Read-Write
        self.type = type

    
    # add site_id of the affected sitte from this transaction
    def add_affected_site_to_transaction(self, site):
        self.sites_affected.add(site)

    # add var of the affected var from this transaction
    def add_affected_var_to_transaction(self, var):
        self.var_affected.add(var)

    # get all affected var from this transaction
    def get_affected_vars_of_transaction(self):
        return self.var_affected

    # get all affected site from this transaction
    def get_affected_sites_of_transaction(self):
        return self.sites_affected

    # add operation to this transaction
    def add_operation(self, op):
        self.operations.append(op)
        self.remaining_operations.append(op)

    # returns all operations orelated to this transaction
    def get_operations(self):
        return self.operations

    # updates the transaction state
    def update_transaction_state(self, state):
        self.transaction_state = state
    
    # returns the type of transaction
    def get_tid(self):
        return self.transaction_id

    # returns the type of transaction
    def get_type(self):
        return self.type

    # return the next operation and pops it from the queue
    def get_next_op(self):
        return self.remaining_operations[0]

    # remove operation from the remaining operation queue
    def remove_op(op, self):
        return self.remaining_operations.remove(op)

