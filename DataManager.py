import DB as db
import Lock as lk

class DataManager:
    def __init__(self, id):

        # site_id
        self.site_id = id

        # Database on this site
        self.db = db.DB()

        # flag to indicate if the site is available
        self.is_available = True

        # variables handled on this site
        self.variables = set()

        # lock map on this site {transaction_id:[list of lock objects]}
        self.lock_map = {} 
        
        # buffer maintains the changes corresponding to uncommited transactions
        self.buffer = {} 

        # map for snapshot of the Database. Used for read only transaction
        self.backups = {} # {t_id:(version_number,db)}
        
        self.initialize_site_vars_and_db(id)

    ''' initialize the site variables and update DB with default values'''
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

    '''Returns the lock map of site'''
    def get_locks(self):
        return self.lock_map

    '''Returns the variables set of this site'''
    def get_vars_of_site(self):
        return self.variables

    '''Returns the site_id'''
    def get_site_id(self):
        return self.site_id

    '''Returns the buffer on the site'''
    def get_site_buffer(self):
        return self.buffer

    '''Remove the uncommited transaction changes from buffer'''
    def remove_from_buffer(self, tid):
        self.buffer.pop(tid)

    '''Returns true if this site handles this variable'''
    def is_var_in_site(self, var):
        return var in self.variables

    '''Write the changes corresponding write operation of this transaction to the buffer '''
    def write_operation(self, tid, var, val):
        if tid not in self.buffer.keys():
            self.buffer[tid] = {}
        self.buffer[tid][var] = val
    
    '''Read the value of the var corresponding read operation of this transaction from the the buffer if 
    there are uncommitted changes from this transaction, otherwise read from db'''
    def read_operation(self,tid,var):
        if tid in self.buffer.keys() and var in self.buffer[tid]:
            print(var," : ",self.buffer[tid][var])
        print(var," : ",self.db.get_value(var))
    
    ''' Read the value of the var at the begin tome of thid Read-only Transaction'''
    def read_only_operation(self, tid, var):
        print(var," : ",self.backups[tid][var])

    ''' Take a snapshot of DB at this site for ReadOnly transaction(tid) and save it to backup map'''
    def snapshot_db(self, tid):
        self.backups[tid] = {};
        for var in self.variables:
            self.backups[tid][var] = self.db.get_value(var)

    ''' Check if this tid can acquire the write lock for this var. If yes return an empty list. 
    Else return the list of transaction currently blocking it from obtaining the lock'''
    def can_acquire_write_lock(self,tid, var):
        blocking_transactions = []
        if var not in self.lock_map.keys():
            return blocking_transactions
        for lock in self.lock_map[var]:
            if lock.get_tid() != tid:
                blocking_transactions.append(lock.get_tid())
        return blocking_transactions

    ''' Acquire the write lock on var for this transaction.'''
    def acquire_write_lock(self, tid, var):
        lock = lk.Lock(lk.Lock_Type.WRITE_LOCK, var,tid)
        if var not in self.lock_map.keys():
            self.lock_map[var] = []
        hasReadLock = False
        for currlock in self.lock_map[var]:
            if lock.get_tid() == currlock.get_tid() and lock.get_lock_type()==currlock.get_lock_type():
                return 
            if lock.get_tid() == currlock.get_tid():
                hasReadLock = True
        if hasReadLock:
            self.upgrade_lock(tid,var)
        else:
            self.lock_map[var].append(lock)

    ''' Check if this tid can acquire the read lock for this var. If yes return an empty list. 
    Else return the list of transaction currently blocking it from obtaining the lock'''
    def can_acquire_read_lock(self, tid, var):
        blocking_transactions = []
        if var not in self.lock_map.keys():
            return blocking_transactions
        for lock in self.lock_map[var]:
            if lock.get_tid() != tid and lock.get_lock_type() == lk.Lock_Type.WRITE_LOCK:
                blocking_transactions.append(lock.get_tid())
        return  blocking_transactions  

    ''' Acquire the read lock on var for this transaction.'''
    def acquire_read_lock(self, tid, var):
        lock = lk.Lock(lk.Lock_Type.READ_LOCK, var, tid)
        if var not in self.lock_map.keys():
            self.lock_map[var] = []
        for currlock in self.lock_map[var]:
            if currlock.get_tid() == tid:
                return
        self.lock_map[var].append(lock)
   
    ''' Release lock of type lock_type on var for this transaction.'''
    def release_lock(self, tid, var, lock_type):
        new_locks = []
        for lock in self.lock_map[var]:
            if lock.get_tid() != tid or lock_type != lock.get_lock_type():
                new_locks.append(lock)
        
        if len(new_locks)>0:
            self.lock_map[var] = new_locks
        else:
            self.lock_map.pop(var)

    ''' Upgrade the read lock to Write lock on var for this transaction(tid)'''
    def upgrade_lock(self, tid, var):
        for lock in self.lock_map[var]:
            if tid == lock.get_tid() and lock.get_lock_type()==lk.Lock_Type.READ_LOCK:
                lock.set_lock_type(lk.Lock_Type.WRITE_LOCK)

    ''' Commit changes to the var of this transaction by writing it to DB from buffer.'''
    def update_database(self, tid, var):
        for var in self.buffer[tid]:
            self.db.update_key(var,self.buffer[tid][var])

    ''' Make the site available'''
    def recover_site(self):
        self.is_available = True

    ''' Make the site Down. Clear the lockmap and Remove all replicated variables from DB. As a rule of thumb, 
    the replicated variables ae not available for read after the site recovers until there  is a successful commit. 
    Therefore the replicaed variables are deleted from the DB when the site fails.'''
    def fail_site(self):
        self.lock_map = {}
        for i in range (1, 21):
            if (i % 2 == 0): 
                var = "x" + str(i)
                self.db.remove_key(var)
        self.is_available = False

    '''Check if the variable open to read operation. '''
    def can_read_var(self, var):
        if self.db.has_key(var):
            return True
        return False

    ''' check if the site is up'''
    def is_site_up(self):
        return self.is_available

    ''' print all the latest commited values to the db'''
    def print_db(self):
        for var in self.variables:
            if self.db.has_key(var):
                print(var, " ",self.db.get_value(var))
            else:
                i = int(var[1:])
                print(var, " ",10*i)

    ''' return the lock held by this transaction on this var. If no lock is there, return None'''
    def get_lock_for_this_transaction_and_var(self, t_id, var):
        if var in self.lock_map:
            lock_list = self.lock_map[var]
            for lock in lock_list:
                if lock.get_tid() == t_id:
                    return lock
        return None

    ''' Return True if snapshot of the db (at the time this transaction began) is available for this transaction'''
    def has_snapshot_for_tid(self, transaction_id):
        return transaction_id in self.backups

    