import Transaction as tr
import TransactionStates as trst
import DataManager as dm
import Operations as opn

class TransactionManager:
    def __init__(self):
        self.time = 0
        self.sites = {}
        self.vars = []
        self.current_transactions = {}

        # map of transaction to list of all transactions waiting on it.
        self.transaction_wait_queue ={}

        # list of all aborted transactions
        self.aborted_transaction  = []

        # list of all transactions
        self.all_transactions = {}

        # list of ended transactions
        self.end_transacion_list = []
        self.initialize_sites(self)
        self.initialize_vars(self)

    ''' Method to initialize all sites in our system'''
    def initialize_sites(self):
        for site in range(1, 11):
            self.sites[site] = dm.DataManager(site)

    ''' Method to initialize all vars in our system'''
    def initialize_vars(self):
        for i in range(1, 21):
            self.vars.append("x"+str(i))

    ''' Method to maintain time.'''
    def tick(self):
        self.time+=1

    ''' Method to handle begin transaction.'''
    def begin_transaction(self, transaction_id, transaction):
        self.current_transactions[transaction_id] = transaction
        self.all_transactions[transaction_id] = transaction
    
    ''' Method to handle write operation for a read write transaction.'''
    def write_operation(self, transaction_id, op):
        var = op.var
        val = op.val
        site_to_be_affected = []

        # find the sites that we can write to
        for site in self.sites.values():
            if var in site.variables and site.is_site_up:
                    site_to_be_affected.append(site)
            # TODO: is this right???
            elif var in site.variables and not site.is_site_up:
                site.add_to_disable_read(var)
        
        # no avvailable sites
        if len(site_to_be_affected) == 0:
            # TODO: put in waitqeue
            pass
        else:
            # check if there are any blocking transactions at any site. If yes then put the transaction in waitqueue, else write to each site.
            blocking_transactions = []
            for site in site_to_be_affected:
                blocking_t = site.can_acquire_write_lock(transaction_id, var)
                blocking_transactions.extend(blocking_t)
            
            # update waitqueue 
            if len(blocking_transactions) > 0:
                for t_id in blocking_transactions:
                    self.transaction_wait_queue[t_id].append(transaction_id) 
            else:
                # no blocking transactions at any site. start write.
                t_obj = self.current_transactions[transaction_id]

                # add affected var to this transaction. Needed to release lock during commit
                if var not in t_obj.var_affected:
                    t_obj.var_affected.append(var)
                
                for site in site_to_be_affected:
                    site.acquire_write_lock(transaction_id, var)
                    site.write_operation(transaction_id, var, val)
                    # add affected site to this transaction. Needed to release lock during commit
                    if site not in t_obj.site_affected:
                        t_obj.site_affected.append(site)
                t_obj.remaining_operations.remove(op)
                t_obj.update_transaction_state(trst.TransactionStates.RUNNING)
    
    ''' Method to handle read operation for a read write transaction.'''
    def read_operation(self, transaction_id, op):
        var = op.var
        site_to_read_from = []
        
        # find the sites that we can read from
        for site in self.sites.values():
            if var in site.variables and site.is_site_up and site.can_read_var(transaction_id, var):
                    site_to_read_from.append(site)
        
        # no available sites
        if len(site_to_read_from) == 0:
            # TODO: put in waitqeue
            pass
        else:
            # check if there are blocking transactions at all sites. If yes then put the transaction in waitqueue, else read value
            blocking_transactions = []
            read_invoked = False
            for site in site_to_read_from:
                blocking_t = site.can_acquire_read_lock(transaction_id, var)
                if blocking_t:
                    blocking_transactions.extend(blocking_t)
                else:
                    # there is a site with no blocking transaction. Read from here
                    t_obj = self.current_transactions[transaction_id]

                    # add affected var to this transaction. Needed to release lock during commit
                    if var not in t_obj.var_affected:
                        t_obj.var_affected.append(var)
                    site.acquire_read_lock(transaction_id, var)
                    site.read_operation(transaction_id, var)
                    # add this site to site affected for this transaction. Needed to release lock during commit
                    if site not in t_obj.site_affected:
                        t_obj.site_affected.append(site)

                    t_obj.remaining_operations.remove(op)
                    t_obj.update_transaction_state(trst.TransactionStates.RUNNING)
                    read_invoked = True
                    break
            
            # we were not able to read from any site. put transaction in wait queue.
            if not read_invoked:
                for t_id in blocking_transactions:
                    self.transaction_wait_queue[t_id].append(transaction_id) 

    ''' Method to handle read operation for a read only transaction.'''
    def read_only_operation(self, transaction_id, op):
        var = op.var
        site_to_read_from = []
        
        # find the sites that we can read from
        for site in self.sites.values():
            if var in site.variables and site.is_site_up and site.can_read_var(transaction_id, var):
                    site_to_read_from.append(site)
        
        # no site available. Abort transaction
        if len(site_to_read_from) == 0:
            self.abort_transaction(transaction_id)
        else:
            # found a site to read from
            for site in site_to_read_from:
                site.read_only_operation(transaction_id, var)
                self.all_transactions[transaction_id].remaining_operations.remove(op)
                self.all_transactions[transaction_id].update_transaction_state(trst.TransactionStates.RUNNING)
                break

    def dump(self):
        print("Printing values of variables at all Sites.\n")
        for site in range(1, 11):
            print("SITE: ", site)
            site_obj = self.sites[site]
            site_obj.print_db()
            print()

    '''Method to handle the end transaction'''
    def end_transaction(self, transaction_id):
        if  transaction_id in self.current_transactions:
            t_obj = self.current_transactions[transaction_id]
            # Readonly transactions. Commit it.
            if t_obj.get_type() == 'RO':
                # # TODO: clear snapshot???
                print("END READONLY Transaction: ", transaction_id)
                t_obj.update_transaction_state(trst.TransactionStates.COMMITED)
            
            # Read-write  transaction
            if t_obj.get_type() == 'RW':

                # check all affected sites are up. if no, then abort the transaction
                if self.all_affected_sites_up(t_obj):
                    print("END READ-WRITE Transaction: ", transaction_id)
                    # TODO: handling disable read flags for varriables???
                    # read writefinal values to all site db.
                    # release locks held by transaction_id
                    self.commit_transaction_and_release_locks(t_obj)
                else:
                    self.abort_transaction(transaction_id)

                # resume the trannsactions waiing on it.
                self.resume_all_waiting_transactions(transaction_id)
            self.current_transactions.pop(transaction_id, None)
            self.transaction_wait_queue.pop(transaction_id, None)
    
    ''' Method to check if al affected sites by a transaction are available.'''
    def all_affected_sites_up(self, transaction):
        for site_id in transaction.sites_affected:
            site = self.sites[site_id]
            if not site.is_site_up():
                return False
        return True
    
    ''' Method to commit all changes and release locks held by this transactions on all sites.'''
    def commit_transaction_and_release_locks(self, transaction):
        for var in transaction.vars_affected:
            for site_id in transaction.sites_affected:
                site = self.sites[site_id]
                lock = site.get_lock_for_this_transaction_and_var(transaction.id, var)
                if lock:
                    # lock is write lock. write to db and release lock
                    if lock.type == 'W':
                        site.update_database(transaction.id, var)
                        site.release_lock(transaction.id, var, 'W')
                    # lock is read lock. release lock
                    if lock.type == 'R':
                        site.release_lock(transaction.id, var, 'R')
        transaction.update_transaction_state(trst.TransactionStates.COMMITED)

    ''' Method to handlle aborting a transaction.'''
    def abort_transaction(self, transaction_id):
        if transaction_id in self.current_transactions:
            t_obj = self.current_transactions[transaction_id]
            #undo all operations
            for op in t_obj.get_operations():
                # undo all Writes. Reads can be ignored
                if op.type == 'W':
                    for site, site_obj in self.sites.items():
                        if site_obj.is_available() and site_obj.is_var_in_site(op.var):
                            #site buffer update.. remove t_id from the buffer and version from map
                            site_obj.buffer.pop(transaction_id)
                            site_obj.backups.pop(transaction_id)
        self.aborted_transaction.append(transaction_id)
        self.current_transactions.pop(transaction_id, None)
        self.all_transactions[transaction_id].update_transaction_state(trst.TransactionStates.ABORTED)
        self.remove_transaction_from_waiting_queue(transaction_id)
        self.resume_all_waiting_transactions(transaction_id)
    
    ''' Method to remove this transaction from all lists of waiting transactions'''
    def remove_transaction_from_waiting_queue(self, transaction_id):
        # remove this transaction from waiting queue of all transactions
        for t_set in self.transaction_wait_queue.values():
            if transaction_id in t_set:
                t_set.remove(transaction_id)

    ''' Method to handle resuming all transactions that were waitinng on transaction:transaction_id'''
    def resume_all_waiting_transactions(self, transaction_id):
        # transactions waiting on transaction:transaction_id
        transactions_list = self.transaction_wait_queue[transaction_id]
        
        for t_id in transactions_list:
            t_obj = self.all_transactions[t_id]
        
            #operation to resume on
            op = t_obj.get_op()

            if op.type == 'W':
                self.write_operation(t_id, op.var, op.val)
            if op.type == 'R':
                self.read_operation(t_id, op.var)
        self.transaction_wait_queue.pop(transaction_id, None)  # t1:[t2, t3]

    ''' Method to fail site'''
    def fail_site(self, site):
        site_obj = self.sites[site]
        site_obj.fail_site(site)

    ''' Method to recover site'''
    def recover_site(self, site):
        site_obj = self.sites[site]
        site_obj.recover_site(site)

    ''' Method to detect a deadlock in wait graph'''
    def deadlock_detect(self):
        pass
    
    ''' Method to clear a deadlock in wait graph'''
    def deadlock_clear(self):
        pass
    
    ''' Method to create snapshots on sites when a Read ONly Transaction begins.'''
    def create_snapshots(self, t_id):
        for site_obj in self.sites.values():
            site_obj.snapshot_db(t_id)
    
    ''' Method to execute instructions coming from a input file'''
    def execute_transaction(self, transaction):
        self.tick()
        op = transaction[0]

        if op == "begin":
            t_id = op[1]
            t_obj = tr.Transaction(t_id, self.time, "RW")
            self.begin_transaction(t_id, t_obj)

        elif op == "beginRO":
            t_id = op[1]
            t_obj = tr.Transaction(t_id, self.time, "RO")
            self.begin_transaction(t_id, t_obj)
            self.create_snapshots(t_id)

        elif op == "end":
            t_id = op[1]
            self.end_transaction(t_id)

        elif op == "W":
            t_id = op[1]
            var = op[2]
            val = op[3]
            operation = opn.Operations('W', self.time, var, val)
            self.all_transactions[t_id].add_operations(operation)
            self.write_operation(t_id, operation)

        elif op == "R":
            t_id = op[1]
            var = op[2]
            operation = opn.Operations('R', self.time, var, val)
            t_obj = self.all_transactions[t_id]
            t_obj.add_operations(operation)
            if t_obj.type == 'RO':
                self.read_only_operation(t_id, operation)
            else:
                self.read_operation(t_id, operation)
        
        elif op == "dump":
            self.dump()

        elif op == "fail":
            site = op[1]
            self.fail_site(site)

        elif op == "recover":
            site = op[1]
            self.recover_site(site)
            


