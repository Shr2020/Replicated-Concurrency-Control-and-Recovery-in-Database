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

    def write_operation(self, transaction_id, variable, value):
        # if all sites down:
            # abort
        #for all sites:
            # site_obj.can_acquire_write_lock(t_id, variable)  : (True, None) if it can, (False, t_id) if not
                #False, t1
                #false, t2
                #waitqueu: t1:t3, t2:t3
        #for all sites:
            # site_obj.acquire_write_lock(t_id, variable)
            # site_obj.write_operation
            # remove operaration form transaction 
        pass
    
    
    def read_operation(self, transaction_id, variable):
        # if all sites down:
            # abort
        #for any sites:
            # site_obj.can_acquire_read_lock(t_id, variable)  : True if itcan, False if not
            #if yes:
                # site_obj.acquire_read_lock(t_id, variable)
                # site_obj.read_operation
        #if false for all:
            #then waitqueueu
        pass

    def read_only_operation(self, transaction_id, variable):
        #site_obj.read_only_operation
        pass

    def dump(self):
        pass

    def end_transaction(self, transaction_id):
        pass

    def abort_transaction(self, transaction_id):
        if transaction_id in self.current_transactions:
            t_obj = self.current_transactions[transaction_id]
            #undo all operations
            for op in t_obj.get_operations():
                # undo all Writes. Reads can be ignored
                if op.type == 'W':
                    for site, site_obj in self.sites.items():
                        if site_obj.is_available() and site_obj.is_var_in_site(op.var):
                            #todo: site buffer update.. remove t_id from the buffer and version from map
                            pass
        self.aborted_transaction.append(transaction_id)
        # todo: change transaction state to aborted
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
            self.all_transactions[t_id].add_operations(opn.Operations('W', self.time, var, val))
            self.write_operation(t_id, var, val)

        elif op == "R":
            t_id = op[1]
            var = op[2]
            self.all_transactions[t_id].add_operations(opn.Operations('R', self.time, var))
            self.read_operation(t_id, var)
        
        elif op == "dump":
            self.dump()

        elif op == "fail":
            site = op[1]
            self.fail_site(site)

        elif op == "recover":
            site = op[1]
            self.recover_site(site)
            


