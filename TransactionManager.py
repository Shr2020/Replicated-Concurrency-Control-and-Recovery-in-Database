import Transaction as tr
import TransactionStates as trst
import DataManager as dm
import Operations as opn

class TransactionManager:
    def __init__(self):
        self.time = 0
        self.sites = {}
        self.vars = []
        #self.var_to_site = {}
        self.current_transactions = {}

        # map of transaction to set of all transactions waiting on it
        self.transaction_wait_queue ={}

        # list of all aborted transactions
        self.aborted_transaction  = []

        # list of all transactions
        self.all_transactions = {}

        # list of ended transactions
        self.end_transacion_list = []
        self.initialize_sites(self)
        self.initialize_vars(self)

    def initialize_sites(self):
        for site in range(1, 11):
            self.sites[site] = dm.DataManager(site)

    def initialize_vars(self):
        for i in range(1, 21):
            self.vars.append("x"+str(i))

    def tick(self):
        self.time+=1

    def begin_transaction(self, transaction_id, transaction):
        self.current_transactions[transaction_id] = transaction
        self.all_transactions[transaction_id] = transaction

    def write_operation(self, transaction_id, op):
        var = op.var
        val = op.val
        site_to_be_affected = []
        
        for site in self.sites.values():
            if var in site.variables and site.is_site_up:
                    site_to_be_affected.append(site)
        
        if len(site_to_be_affected) == 0:
            # todo: put in waitqeue
            pass
        else:
            blocking_transactions = []
            for site in site_to_be_affected:
                blocking_t = site.can_acquire_write_lock(transaction_id, var)
                blocking_transactions.extend(blocking_t)

            if len(blocking_transactions) > 0:
                for t_id in blocking_transactions:
                    self.transaction_wait_queue[t_id].append(transaction_id) 
            else:
                for site in site_to_be_affected:
                    site.acquire_write_lock(transaction_id, var)
                    site.write_operation(transaction_id, var, val)
                self.all_transactions[transaction_id].remaining_operations.remove(op)
                self.all_transactions[transaction_id].update_transaction_state(trst.TransactionStates.RUNNING)
    
    
    def read_operation(self, transaction_id, op):
        var = op.var
        site_to_read_from = []
        
        for site in self.sites.values():
            if var in site.variables and site.is_site_up and site.can_read_var(transaction_id, var):
                    site_to_read_from.append(site)
        
        if len(site_to_read_from) == 0:
            # todo: put in waitqeue
            pass
        else:
            blocking_transactions = []
            for site in site_to_read_from:
                blocking_t = site.can_acquire_read_lock(transaction_id, var)
                if len(blocking_t) > 0:
                    blocking_transactions.append(blocking_t)
                else:
                    site.acquire_write_lock(transaction_id, var)
                    site.read_operation(transaction_id, var)
                    self.all_transactions[transaction_id].remaining_operations.remove(op)
                    self.all_transactions[transaction_id].update_transaction_state(trst.TransactionStates.RUNNING)
                    break
            
            if len(blocking_transactions) == len(site_to_read_from):
                for t_id in blocking_transactions:
                    self.transaction_wait_queue[t_id].append(transaction_id) 

    def read_only_operation(self, transaction_id, op):
        var = op.var
        site_to_read_from = []
        
        for site in self.sites.values():
            if var in site.variables and site.is_site_up and not site.disable_read:
                    site_to_read_from.append(site)
        
        if len(site_to_read_from) == 0:
            self.abort_transaction(transaction_id)
        else:
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

    def end_transaction(self, transaction_id):
        if  transaction_id in self.current_transactions:
            t_obj = self.current_transactions[transaction_id]
            if t_obj.get_type() == 'RO':
                print("END READONLY Transaction: ", transaction_id)
            if t_obj.get_type() == 'RW':
                if self.all_affected_sites_up(t_obj):
                    print("END READ-WRITE Transaction: ", transaction_id)
                    # read writefinal values to all site db.
                    # release locks held by transaction_id
                    self.commit_transaction_and_release_locks(t_obj)
                else:
                    self.abort_transaction(transaction_id)
                self.resume_all_waiting_transactions(transaction_id)
            self.current_transactions.pop(transaction_id, None)
            self.transaction_wait_queue.pop(transaction_id, None)
    
    def all_affected_sites_up(self, transaction):
        for site_id in transaction.sites_affected:
            site = self.sites[site_id]
            if not site.is_site_up():
                return False
        return True
                
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
        
    def remove_transaction_from_waiting_queue(self, transaction_id):
        # remove this transaction from waiting queue of all transactions
        for t_set in self.transaction_wait_queue.values():
            if transaction_id in t_set:
                t_set.remove(transaction_id)

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

    def fail_site(self, site):
        site_obj = self.sites[site]
        site_obj.fail_site(site)

    def recover_site(self, site):
        site_obj = self.sites[site]
        site_obj.recover_site(site)

    def deadlock_detect(self):
        pass

    def deadlock_clear(self):
        pass

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
            operation = opn.Operations('W', self.time, var, val)
            self.all_transactions[t_id].add_operations(operation)
            self.read_operation(t_id, operation)
        
        elif op == "dump":
            self.dump()

        elif op == "fail":
            site = op[1]
            self.fail_site(site)

        elif op == "recover":
            site = op[1]
            self.recover_site(site)
            


