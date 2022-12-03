import Value as vl
class DB:
    def __init__(self):
        # version of the DB.
        self.version = 1

        # key-value store 
        self.kv = {}
    
    # update value of key in key-value store 
    def update_key(self, key, val,commit_time):
        value = vl.Value(val,commit_time)
        if key not in self.kv.keys():
            self.kv[key] = []
        self.kv[key].append(value)

    # get value of key from key-value store 
    def get_value(self, key):
        if key in self.kv:
            index = len(self.kv[key])-1
            return self.kv[key][index].getVal();
        return None

    # check if key-value store has key
    def has_key(self, key):
        return key in self.kv
    
    # remove key from key-value store
    def remove_key(self,key):
        self.kv.pop(key,None)

    # increment version of db 
    def increment_version(self):
        self.version+=1

    # get version of db 
    def get_version(self):
        return self.version