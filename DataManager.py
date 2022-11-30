import DB
class DataManager:
    def __init__(self, id):
        self.db = DB()
        self.is_available = True
        self.lock_map = {} 
        self.site_id = id
        self.buffer = {} #t_id:(var, val)
        self.backups = {} # t_id:(version_number,db)
        self.variables = set()
        self.initialize_site_vars_and_db(id)
        self.disable_read = False
        
    '''
    Data
    The data consists of 20 distinct variables x1, ..., x20 (the numbers between 1 and 20 will be referred to as indexes below). There are 10 sites
    numbered 1 to 10. A copy is indicated by a dot. Thus, x6.2 is the copy of
    variable x6 at site 2. The odd indexed variables are at one site each (i.e. 1 
    + (index number mod 10) ). For example, x3 and x13 are both at site 4.
    Even indexed variables are at all sites. Each variable xi is initialized to the
    value 10i (10 times i). Each site has an independent lock table. If that site
    fails, the lock table is erased.
    '''
    def initialize_site_vars_and_db(self, id):
        listt = []
        for i in range (1, 21):
            if (i % 2 == 0):
                listt.append("x" + str(i))
                self.db.update_key(i, 10*i)
            else:
                if (id == 1 + (i % 10)):
                    listt.append("x" + str(i))
                    self.db.update_key(i, 10*i)
        self.variables = set(listt)

    def is_var_in_site(self, var):
        return var in self.variables
    # check if res list has same transaction
    def can_acquire_write_lock(transaction_id, var):
        if var not in self.lock_map.keys():
            return []
        
        return self.lock_map[var]

    def acquire_write_lock(tid, var):
        lock = Lock("W",tid,var)
        if var not in self.lock_map.keys():
            self.lock_map[var] = []
        for currlock in self.lock_map[var]:
            if lock.t_id == currLock.t_id and lock.lock_type==currLock.lock_type:
                return   
        self.lock_map[var].append(lock)

    def can_acquire_read_lock(tid, var):
        blocking_transactions = []
        if var not in self.lock_map.keys():
            return blocking_transactions
        for lock in self.lock_map[var]:
            if lock.t_id != tid and lock_type == "W":
                blocking_transactions.append(lock)
        return  blocking_transactions  

    def acquire_read_lock(self, tid, var):
        lock = Lock("R",tid,var)
        if var not in self.lock_map.keys():
            self.lock_map[var] = []
            
        for currlock in self.lock_map[var]:
            if currlock.t_id == tid:
                return
        self.lock_map[var].append(lock)
   
    def release_lock(self, var, lock_type):
        new_locks = []
        for lock in self.lock_map[var]:
            if lock.t_id != tid or lock_type != lock.lock_type:
                new_locks.append(lock)
        
        if len(new_locks)>0:
            self.lock_map[var] = new_locks
        else:
            self.lock_map.pop(var)

    def upgrade_lock(self, var):
        pass

    def update_database(self, t_id, var):
        pass

    def recover_site(self):
        self.is_available = True

    def fail_site(self):
        self.is_available = True

    def is_site_up(self):
        return self.is_available

    def print_db(self):
        self.db.print_kv()

    def get_locks(self):
        return self.lock_map

    def get_lock_for_this_transaction_and_var(self, t_id, var):
        if var in self.lock_map:
            lock_list = self.lock_map[var]
            for lock in lock_list:
                if lock.t_id == t_id:
                    return lock
        return None