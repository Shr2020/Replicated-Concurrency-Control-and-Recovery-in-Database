from enum import Enum

class Lock_Type(Enum):
    READ_LOCK = 0 
    WRITE_LOCK = 1  

class Lock:
    def __init__(self, lock_type, var, tid):

        # Lock type. R: Read Lock, W: write Lock
        self.lock_type = lock_type

        # variable associated with this lock
        self.var = var

        # transaction which holds his lock
        self.t_id = tid

    # get the lock-type (Read/Write)
    def get_lock_type(self):
        return self.lock_type

    # get the lock-type (Read/Write)
    def set_lock_type(self, lock_type):
        self.lock_type = lock_type

    # get the variable associated to lock
    def get_locked_var(self):
        return self.var

    # get the transaction id associated to lock
    def get_tid(self):
        return self.t_id

 