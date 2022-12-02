import DB as db
import Lock as lk
class DataManager:
    def __init__(self, id):
        self.db = db.DB()
        self.is_available = True
        self.lock_map = {} 
        self.site_id = id
        self.buffer = {} 
        self.backups = {} # t_id:(version_number,db)
        self.variables = set()
        self.initialize_site_vars_and_db(id)
        self.disable_read = set()
        
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
                var = "x" + str(i)
                listt.append(var)
                self.db.update_key(var, 10*i)
            else:
                if (id == 1 + (i % 10)):
                    var = "x" + str(i)
                    listt.append(var)
                    self.db.update_key(var, 10*i)
        self.variables = set(listt)

    def is_var_in_site(self, var):
        return var in self.variables

    def write_operation(self,tid, var, val):
        if tid not in self.buffer.keys():
            self.buffer[tid] = {}
        self.buffer[tid][var] = val
        
        

    def read_operation(self,tid,var):
        if tid in self.buffer.keys() and var in self.buffer[tid]:
            print(var," : ",self.buffer[tid][var])
        print(var," : ",self.db.get_value(var))
    
    def read_only_operation(self, tid, var):
        print(var," : ",self.backups[tid][var])

    def snapshot_db(self, tid):
        self.backups[tid] = {};
        for var in self.variables:
            self.backups[tid][var] = self.db.get_value(var)


    # check if res list has same transaction
    def can_acquire_write_lock(self,tid, var):
        blocking_transactions = []
        if var not in self.lock_map.keys():
            return blocking_transactions
        for lock in self.lock_map[var]:
            if lock.t_id != tid:
                blocking_transactions.append(lock.t_id)
        return blocking_transactions

    def acquire_write_lock(self, tid, var):
        lock = lk.Lock("W", var,tid)
        if var not in self.lock_map.keys():
            self.lock_map[var] = []
        hasReadLock = False
        for currlock in self.lock_map[var]:
            if lock.t_id == currlock.t_id and lock.lock_type==currlock.lock_type:
                return 
            if lock.t_id == currlock.t_id:
                hasReadLock = True
        if hasReadLock:
            self.upgrade_lock(tid,var)
        else:
            self.lock_map[var].append(lock)

    def can_acquire_read_lock(self, tid, var):
        blocking_transactions = []
        if var not in self.lock_map.keys():
            return blocking_transactions
        for lock in self.lock_map[var]:
            if lock.t_id != tid and lock.lock_type == "W":
                blocking_transactions.append(lock.t_id)
        return  blocking_transactions  

    def acquire_read_lock(self, tid, var):
        lock = lk.Lock("R",var,tid)
        if var not in self.lock_map.keys():
            self.lock_map[var] = []
        for currlock in self.lock_map[var]:
            if currlock.t_id == tid:
                return
        self.lock_map[var].append(lock)
   
    def release_lock(self,tid, var, lock_type):
        new_locks = []
        for lock in self.lock_map[var]:
            if lock.t_id != tid or lock_type != lock.lock_type:
                new_locks.append(lock)
        
        if len(new_locks)>0:
            self.lock_map[var] = new_locks
        else:
            self.lock_map.pop(var)

    def upgrade_lock(self,tid,var):
        for lock in self.lock_map[var]:
            if tid == lock.t_id and lock.lock_type=='R':
                lock.lock_type='W'

    def update_database(self, tid, var):
        for var in self.buffer[tid]:
            self.db.update_key(var,self.buffer[tid][var])

    def recover_site(self):
        self.is_available = True


    def fail_site(self):
        self.lock_map = {}
        for i in range (1, 21):
            if (i % 2 == 0): 
                var = "x" + str(i)
                self.db.remove_key(var)
        self.is_available = False

    def can_read_var(self, transaction_id, var):
        if self.db.has_key(var):
            return True
        return False

    def remove_disable_flag_for_var(self, transaction_id, var):
        if var in self.disable_read:
            self.disable_read.remove(var)

    def is_site_up(self):
        return self.is_available

    def print_db(self):
        for var in self.variables:
            if self.db.has_key(var):
                print(var, " ",self.db.get_value(var))
            else:
                i = int(var[1:])
                print(var, " ",10*i)

    def get_locks(self):
        return self.lock_map

    def get_lock_for_this_transaction_and_var(self, t_id, var):
        if var in self.lock_map:
            lock_list = self.lock_map[var]
            for lock in lock_list:
                if lock.t_id == t_id:
                    return lock
        return None

    def add_to_disable_read(self, var):
        self.disable_read.add(var)

    def has_snapshot_for_tid(self, transaction_id):
        return transaction_id in self.backups

    