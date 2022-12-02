class DB:
    def __init__(self):
        # version of the DB.
        self.version = 1

        # key-value store 
        self.kv = {}
    
    def update_key(self, key, val):
        self.kv[key] = val

    def get_value(self, key):
        if key in self.kv:
            return self.kv[key]
        return None

    def has_key(self, key):
        return key in self.kv
    def remove_key(self,key):
        self.kv.pop(key,None)

    def increment_version(self):
        self.version+=1

    def get_version(self):
        return self.version