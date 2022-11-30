class Lock:
    def __init__(self, lock_type, var, tid):

        # Lok type. R: Read Lock, W: write Lock
        self.lock_type = lock_type

        # variable associated with this lock
        self.var = var

        # trnsaction which holds his lock
        self.t_id = tid