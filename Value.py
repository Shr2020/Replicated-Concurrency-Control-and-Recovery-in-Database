class Value:
    def __init__(self, val, commit_time):

        # operation type (Read/ Write)
        self.val = val

        # start time of operation
        self.commit_time = commit_time
    def getVal(self):
        return self.val
