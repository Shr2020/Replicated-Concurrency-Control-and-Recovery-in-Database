from enum import Enum

class OP_Type(Enum):
    READ = 0 
    WRITE = 1  

class Operation:
    def __init__(self, type, time, tid, var, val=None):

        # operation type (Read/ Write)
        self.op_type = type

        # start time of operation
        self.start_time = time

        # variable associated to operation
        self.var = var

        #value associated to operation
        self.val = val

        #transaction associated to operation
        self.tid = tid

    # get the start time of operation
    def get_start_time(self):
        return self.start_time

    # get the variable associated to operation
    def get_var(self):
        return self.var

    # get the value associated to operation
    def get_val(self):
        return self.val

    # get the transaction id associated to operation
    def get_tid(self):
        return self.tid

    # get the operation-type (Read/Write)
    def get_op_type(self):
        return self.op_type