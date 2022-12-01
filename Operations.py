class Operations:
    def __init__(self, type, time, tid, var, val=None):
        self.op_type = type
        self.start_time = time
        self.var = var
        self.val = val
        self.tid = tid