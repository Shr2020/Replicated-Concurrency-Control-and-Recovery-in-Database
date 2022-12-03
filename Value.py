class Value:
    def __init__(self, val, commit_time):

        # value to be assigned
        self.val = val

        # start time of operation
        self.commit_time = commit_time

    # get value
    def getVal(self):
        return self.val
