class Lock:
    def __init__(self, lock_type, var, tid):
        self.lock_type = lock_type
        self.var = var
        self.t_id = tid