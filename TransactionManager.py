import Transaction as tr
import DataManager as dm
import Operations as opn
import Lock as lk

class TransactionManager:
    def __init__(self):
        # time as integer
        self.time = 0

        # Map of site_id to DataManager Object
        self.sites = {}

        # List of all variables
        self.vars = []

        # Map of ongoing transaction id to its transaction objec
        self.current_transactions = {}

        # Map of all transaction id to their transaction object
        self.all_transactions = {}

        # Map of a variable to map of transaction_id to list of transactions waiting on it.  {var:{t:[list of t' waiting on t]}}
        self.transaction_wait_queue ={}
        
        # map of transactions to sites which could start because all sites are down
        self.transaction_waiting_for_site ={}

        # list of all aborted transactions
        self.aborted_transaction  = []

        # list of ended transactions
        self.end_transacion_list = []

        self.initialize_sites()
        self.initialize_vars()
        self.initialize_wait_queue()

    ''' Method to initialize all sites in our system'''
    def initialize_sites(self):
        for site in range(1, 11):
            self.sites[site] = dm.DataManager(site)

    ''' Method to initialize all vars in our system'''
    def initialize_vars(self):
        for i in range(1, 21):
            self.vars.append("x"+str(i))

    ''' Method to initialize the ait_queue'''
    def initialize_wait_queue(self):
        for var in self.vars:
            self.transaction_wait_queue[var] = {}

    ''' Method to maintain time. Increments time every time the method is called.'''
    def tick(self):
        self.time+=1

    ''' Method to handle begin transaction.'''
    def begin_transaction(self, transaction_id, transaction):
        self.current_transactions[transaction_id] = transaction
        self.all_transactions[transaction_id] = transaction
    
    ''' Method to handle write operation for a read write transaction.'''
    def write_operation(self, transaction_id, op):
        if self.current_transactions[transaction_id].transaction_state != tr.TransactionStates.TO_BE_ABORTED:
            var = op.get_var()
            val = op.get_val()
            site_to_be_affected = []
            sites_handling_this_var = []
            # find the sites that we can read from
            for site in self.sites.values():
                site_variables = site.get_vars_of_site()
                if var in site_variables:
                    sites_handling_this_var.append(site.get_site_id())
                    if site.is_site_up():
                        site_to_be_affected.append(site)
                        
            
            # no available sites
            if len(site_to_be_affected) == 0:
                self.transaction_waiting_for_site[transaction_id] = sites_handling_this_var
                print("Transactions:",transaction_id," is waiting for sites to be available for write operation")
                return False
            else:
                # check if there are any blocking transactions at any site. If yes then put the transaction in waitqueue, else write to each site.
                blocking_transactions = set()
                for site in site_to_be_affected:
                    blocking_t = site.can_acquire_write_lock(transaction_id, var)
                    blocking_transactions.update(blocking_t)
                if len(blocking_transactions) == 0 and self.transaction_wait_queue[var] != {}:
                    for t in self.transaction_wait_queue[var]:
                        # list of tansactions waiting for t. our current transaction has to wait for the already waiting transactions to complete first.(first-come-first-serve)
                        wt_list = self.transaction_wait_queue[var][t]
                        blocking_transactions.update(wt_list)        
                
                # update waitqueue 
                if len(blocking_transactions) > 0:
                    for t_id in blocking_transactions:
                        if t_id not in self.transaction_wait_queue[var]:
                            self.transaction_wait_queue[var][t_id] = set()
                        self.transaction_wait_queue[var][t_id].add(transaction_id)
                        print("Transactions:", transaction_id," is waiting for:", blocking_transactions)
                        return False
                else:
                    # no blocking transactions at any site. start write.
                    t_obj = self.current_transactions[transaction_id]
                        
                    # add affected var to this transaction. Needed to release lock during commit
                    t_obj.add_affected_var_to_transaction(var)
                    for site in site_to_be_affected:
                        site.acquire_write_lock(transaction_id, var)
                        site.write_operation(transaction_id, var, val)
                        # add affected site to this transaction. Needed to release lock during commit
                        if site.get_site_id() not in t_obj.get_affected_sites_of_transaction():
                            t_obj.add_affected_site_to_transaction(site.get_site_id())
                    print("WRITE OPERATION on buffer (uncommited) by", transaction_id, "on", var, "with val", val, "on sites:", [x.site_id for x in site_to_be_affected])
                    t_obj.remove_op(op)
                    t_obj.update_transaction_state(tr.TransactionStates.RUNNING)
                    return True
        return False
    
    
    ''' Method to handle read operation for a read write transaction.'''
    def read_operation(self, transaction_id, op):
        if self.current_transactions[transaction_id].transaction_state != tr.TransactionStates.TO_BE_ABORTED:
            var = op.get_var()
            site_to_read_from = []
            sites_handling_this_var = []
            # find the sites that we can read from
            for site in self.sites.values():
                site_variables = site.get_vars_of_site()
                if var in site_variables:
                    sites_handling_this_var.append(site.get_site_id())
                    if site.is_site_up and site.can_read_var(var):
                        site_to_read_from.append(site)
            
            # no available sites
            if len(site_to_read_from) == 0:
                self.transaction_waiting_for_site[transaction_id] = sites_handling_this_var
                print("Transactions:",transaction_id," is waiting for sites for read operation")
                return False
            else:
                # check if there are blocking transactions at all sites. If yes then put the transaction in waitqueue, else read value
                blocking_transactions = set()
                read_invoked = False
                added_to_waiting = False
                for site in site_to_read_from:
                    blocking_t = site.can_acquire_read_lock(transaction_id, var)
                    blocking_transactions.update(blocking_t)
                    if len(blocking_t)==0:
                        # check if this transaction should wait for any other waiting transaction
                        if self.transaction_wait_queue[var] != {}:
                            added_to_waiting = True
                            
                        else:
                            # there is a site with no blocking transaction. Read from here
                            t_obj = self.current_transactions[transaction_id]

                            # add affected var to this transaction. Needed to release lock during commit
                            t_obj.add_affected_var_to_transaction(var)
                            site.acquire_read_lock(transaction_id, var)
                            site.read_operation(transaction_id, var)
                            # add this site to site affected for this transaction. Needed to release lock during commit
                            if site.get_site_id() not in t_obj.get_affected_sites_of_transaction():
                                t_obj.add_affected_site_to_transaction(site.get_site_id())

                            t_obj.remove_op(op)
                            t_obj.update_transaction_state(tr.TransactionStates.RUNNING)
                            added_to_waiting = False
                            read_invoked = True
                            return True
                if added_to_waiting:
                    for t in self.transaction_wait_queue[var]:
                    # list of tansactions waiting for t. our current transaction has to wait for the already waiting transactions to complete first.(first-come-first-serve)
                        wt_list = self.transaction_wait_queue[var][t]
                        #{x:{T1(W):[T2(W), T3(R)]}, curr T4(R, x).. then T4 should wait on T2 only
                        for wt in wt_list:
                            if (self.all_transactions[wt].get_next_op().type() == opn.OP_Type.WRITE):
                                blocking_transactions.add(wt)
                # we were not able to read from any site. put transaction in wait queue.
                if not read_invoked:
                    print("Transactions:",transaction_id," is waiting for: ",blocking_transactions)
                    for t_id in blocking_transactions:
                        if t_id not in self.transaction_wait_queue[var]:
                            self.transaction_wait_queue[var][t_id] = set()
                        self.transaction_wait_queue[var][t_id].add(transaction_id) 
        return False

    ''' Method to handle read operation for a read only transaction.'''
    def read_only_operation(self, transaction_id, op):
        var = op.get_var()
        # find the sites that we can read from
        for site in self.sites.values():
            if var in site.get_vars_of_site() and site.has_snapshot_for_tid(transaction_id):
                site.read_only_operation(transaction_id, var)
                self.all_transactions[transaction_id].remove_op(op)
                self.all_transactions[transaction_id].update_transaction_state(tr.TransactionStates.RUNNING)
                return 
        # no site available. Abort transaction
        self.abort_transaction(transaction_id)
        print("Read only Transaction aborted: ",transaction_id," due to all sites down")
        

    def dump(self):
        print("\nDUMP: Printing values of variables at all Sites.\n")
        for site in range(1, 11):
            print("SITE:", site)
            site_obj = self.sites[site]
            if not site_obj.is_site_up():
                print("Site is not available")
            site_obj.print_db()
            print()

    '''Method to handle the end transaction'''
    def end_transaction(self, transaction_id):
        
        t_obj = self.all_transactions[transaction_id]
        if (t_obj.transaction_state == tr.TransactionStates.TO_BE_ABORTED):
                print("Transaction ABORTED at commit time : ",transaction_id)
                self.abort_transaction(transaction_id)
                # resume the trannsactions waiing on it.
                self.remove_transaction_from_waiting_queue(transaction_id)
                self.resume_all_waiting_transactions(transaction_id)
        elif  transaction_id in self.current_transactions:
            t_obj = self.current_transactions[transaction_id]
            # Readonly transactions. Commit it.
            if t_obj.get_type() == tr.TransactionType.READ_ONLY:
                # # TODO: clear snapshot???
                print("END READONLY Transaction:", transaction_id)
                t_obj.update_transaction_state(tr.TransactionStates.COMMITED)
                self.end_transacion_list.append(transaction_id)
            # Read-write  transaction
            if t_obj.get_type() == tr.TransactionType.READ_WRITE:

                # check all affected sites are up. if no, then abort the transaction
                if self.all_affected_sites_up(t_obj):
                    print("END READ-WRITE Transaction:", transaction_id)
                    # read writefinal values to all site db.
                    # release locks held by transaction_id
                    self.commit_transaction_and_release_locks(t_obj)
                    self.end_transacion_list.append(transaction_id)
                    print("Transaction COMMITED:",transaction_id)
                    
                else:
                    self.abort_transaction(transaction_id)

                # resume the transactions waiing on it.
                self.remove_transaction_from_waiting_queue(transaction_id)
                self.resume_all_waiting_transactions(transaction_id)
        self.current_transactions.pop(transaction_id, None)
    
    ''' Method to check if al affected sites by a transaction are available.'''
    def all_affected_sites_up(self, transaction):
        for site_id in transaction.get_affected_sites_of_transaction():
            site = self.sites[site_id]
            if not site.is_site_up():
                return False
        return True
    
    ''' Method to commit all changes and release locks held by this transactions on all sites.'''
    def commit_transaction_and_release_locks(self, transaction):
        for var in transaction.get_affected_vars_of_transaction():
            is_write_op = False
            v = 0
            sa = []
            for site_id in transaction.get_affected_sites_of_transaction():
                site = self.sites[site_id]
                lock = site.get_lock_for_this_transaction_and_var(transaction.get_tid(), var)
                if lock:
                    # lock is write lock. write to db and release lock
                    if lock.lock_type == lk.Lock_Type.WRITE_LOCK:
                        is_write_op = True
                        sa.append(site_id)
                        v = site.update_database(transaction.get_tid(), var, self.time)
                        site.release_lock(transaction.get_tid(), var, lk.Lock_Type.WRITE_LOCK)
                    
                    # lock is read lock. release lock
                    if lock.lock_type == lk.Lock_Type.READ_LOCK:
                        site.release_lock(transaction.get_tid(), var, lk.Lock_Type.READ_LOCK)
            if is_write_op:
                print("WRITE OPERATION COMMITED for Transaction", transaction.get_tid(), "for variable", var, "with val", v, "on sites:", sa)

        
        # update transaction status to commited
        transaction.update_transaction_state(tr.TransactionStates.COMMITED)

    ''' Method to handlle aborting a transaction.'''
    def abort_transaction(self, transaction_id):
        if transaction_id in self.current_transactions:
            t_obj = self.current_transactions[transaction_id]
            #undo all operations of this transaction
            for op in t_obj.get_operations():
                # undo all Writes. Reads can be ignored
                if op.get_op_type() == opn.OP_Type.WRITE:
                    for site_id in t_obj.get_affected_sites_of_transaction():
                        site_obj = self.sites[site_id]
                        if site_obj.is_site_up() and site_obj.is_var_in_site(op.get_var()):
                            #site buffer update.. remove t_id from the buffer and version from map
                            if transaction_id in site_obj.get_site_buffer():
                                site_obj.remove_from_buffer(transaction_id)
    
            self.aborted_transaction.append(transaction_id)
            self.current_transactions.pop(transaction_id, None)
            self.all_transactions[transaction_id].update_transaction_state(tr.TransactionStates.ABORTED)
            
            # release all locks for this transaction
            for var in t_obj.get_affected_vars_of_transaction():
                for site_id in t_obj.get_affected_sites_of_transaction():
                    site = self.sites[site_id]
                    if site.is_var_in_site(var):
                        lock = site.get_lock_for_this_transaction_and_var(t_obj.get_tid(), var)
                        if lock:
                        # lock is write lock. write to db and release lock
                            if lock.get_lock_type() == lk.Lock_Type.WRITE_LOCK:
                                site.release_lock(t_obj.get_tid(), var, lk.Lock_Type.WRITE_LOCK)
                        # lock is read lock. release lock
                            if lock.get_lock_type() == lk.Lock_Type.READ_LOCK:
                                site.release_lock(t_obj.get_tid(), var, lk.Lock_Type.READ_LOCK)
            
            # cleanup waiting queue and resume transactions
            self.remove_transaction_from_waiting_queue(transaction_id)
            self.resume_all_waiting_transactions(transaction_id)
            print("Transaction ABORTED at commit time : ",transaction_id)

    ''' Method to remove this transaction from all lists of waiting transactions'''
    def remove_transaction_from_waiting_queue(self, transaction_id):
        # remove this transaction from waiting queue of all transactions
        for var in self.transaction_wait_queue:
            for curr_tid in self.transaction_wait_queue[var]:
                if transaction_id in self.transaction_wait_queue[var][curr_tid]:
                    self.transaction_wait_queue[var][curr_tid].remove(transaction_id)
                    if len(self.transaction_wait_queue[var][curr_tid])==0:
                        self.transaction_wait_queue[var] = {}

    ''' Method to cleanup the keys of wait_queue for each var'''
    def cleanup_waiting_queue(self, transaction_id):
        # remove this transaction key from hashmap of alll variables
        for var in self.transaction_wait_queue:
            self.transaction_wait_queue[var].pop(transaction_id, None)

    '''Method to handle resuming the transactions that are waiting for the sites to be recovered.'''
    def resume_all_site_waiting_transactions(self):
        transactions_list = set()
        for t_id, sites in self.transaction_waiting_for_site.items():
            for site in sites:
                if self.sites[site].is_site_up():
                    transactions_list.add(t_id)
                    break
        return transactions_list

    ''' Method to handle resuming all transactions that were waiting on this transaction'''
    def resume_all_waiting_transactions(self, transaction_id):
        # transactions waiting on transaction:transaction_id
        transactions_list = set()
        for var in self.transaction_wait_queue:
            if transaction_id in self.transaction_wait_queue[var]:
                transactions_list.update(self.transaction_wait_queue[var][transaction_id])

        op_list = []
        self.cleanup_waiting_queue(transaction_id) 
        #check all ransactions waiting for sites to be availablle and add it to the op list
        transactions_waiting_for_site = self.resume_all_site_waiting_transactions()
        transactions_list.update(transactions_waiting_for_site)
        for t_id in transactions_list:
            t_obj = self.current_transactions[t_id]
            op = t_obj.get_next_op()
            op_list.append(op)
        op_list.sort(key=lambda x: x.start_time)
        for op in op_list:
            res = False
            if op.get_op_type() == opn.OP_Type.WRITE:
                res = self.write_operation(op.get_tid(), op)
            if op.get_op_type() == opn.OP_Type.READ:
                res = self.read_operation(op.get_tid(), op)
            if res and op.get_tid() in transactions_waiting_for_site:
                if op.get_op_type() == opn.OP_Type.READ:
                    print("Transaction ", op.get_tid(),"performed READ OPERATION after waiting for site to be available")
                self.transaction_waiting_for_site.pop(op.get_tid())

    ''' Method to fail site'''
    def fail_site(self, site):
        
        site_obj = self.sites[site]
        site_obj.fail_site()
        for t_id in self.current_transactions:
            t_obj = self.current_transactions[t_id]
            if site in t_obj.get_affected_sites_of_transaction():
                t_obj.update_transaction_state(tr.TransactionStates.TO_BE_ABORTED)

    ''' Method to recover site'''
    def recover_site(self, site):
        site_obj = self.sites[site]
        site_obj.recover_site()
    
    ''' Method to create the graph from the transaction_wait_queue map '''
    def deadlock_graph(self):
        deadlock_graph = {}
        for var in self.transaction_wait_queue:
            for tid in self.transaction_wait_queue[var]:
                if  tid not in deadlock_graph:
                    deadlock_graph[tid] = set()
                deadlock_graph[tid].update(self.transaction_wait_queue[var][tid])
        total_visited = set()
        for tid in deadlock_graph:
            if tid not in total_visited:
                visited = {}
                rec_stack = []
                self.deadlock_detect(tid, visited, rec_stack,deadlock_graph,total_visited)

    ''' Method to detect a deadlock in wait graph'''
    def deadlock_detect(self, tid, visited, rec_stack, deadlock_graph, total_visited):
        if tid in deadlock_graph and self.all_transactions[tid].transaction_state != tr.TransactionStates.ABORTED :
            total_visited.add(tid)
            visited[tid] = len(rec_stack) + 1
            rec_stack.append(tid)
            for curr_tid in deadlock_graph[tid]:
                if curr_tid in visited:
                    self.deadlock_clear(rec_stack, visited[curr_tid] - 1)
                else:
                    self.deadlock_detect(curr_tid, visited, rec_stack,deadlock_graph,total_visited)

            rec_stack.pop()
            visited.pop(tid)
    
    ''' Method to clear a deadlock in wait graph'''
    def deadlock_clear(self, trans_list, index):
        transaction_id_list = trans_list[index:]
        youngest_id = -1
        youngest_time = -1;
        for t_id in transaction_id_list:
            if self.all_transactions[t_id].time > youngest_time:
                youngest_id = t_id
                youngest_time = self.all_transactions[t_id].time
        print("ABORTED youngest transaction due to deadlock: ", youngest_id)
        self.abort_transaction(youngest_id)
        return youngest_id

    
    ''' Method to create snapshots on sites when a Read ONly Transaction begins.'''
    def create_snapshots(self, t_id):
        for site_obj in self.sites.values():
            if site_obj.is_site_up:
                site_obj.snapshot_db(t_id)
    
    ''' Method to execute instructions coming from a input file'''
    def execute_transaction(self, transaction):
        self.tick()
        self.deadlock_graph()

        op = transaction[0].strip()

        if op == "begin":
            t_id = transaction[1].strip()
            t_obj = tr.Transaction(t_id, self.time, tr.TransactionType.READ_WRITE)
            self.begin_transaction(t_id, t_obj)

        elif op == "beginRO":
            t_id = transaction[1].strip()
            t_obj = tr.Transaction(t_id, self.time, tr.TransactionType.READ_ONLY)
            self.begin_transaction(t_id, t_obj)
            self.create_snapshots(t_id)

        elif op == "end":
            t_id = transaction[1].strip()
            self.end_transaction(t_id)

        elif op == "W":
            t_id = transaction[1].strip()
            var = transaction[2].strip()
            val = transaction[3].strip()
            operation = opn.Operation(opn.OP_Type.WRITE, self.time, t_id, var, int(val))
            self.all_transactions[t_id].add_operation(operation)
            self.write_operation(t_id, operation)

        elif op == "R":
            t_id = transaction[1].strip()
            var = transaction[2].strip()
            operation = opn.Operation(opn.OP_Type.READ, self.time, t_id, var, None)
            t_obj = self.all_transactions[t_id]
            t_obj.add_operation(operation)
            if t_obj.get_type() == tr.TransactionType.READ_ONLY:
                self.read_only_operation(t_id, operation)
            else:
                self.read_operation(t_id, operation)
        
        elif op == "dump":
            self.dump()

        elif op == "fail":
            site = transaction[1].strip()
            self.fail_site(int(site))

        elif op == "recover":
            site = transaction[1].strip()
            self.recover_site(int(site))
            


