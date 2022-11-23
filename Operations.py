class Operations:
    def __init__(self, type, time, var, val=None) -> None:
        self.op_type = type
        self.start_time = time
        self.var = var
        self.val = val