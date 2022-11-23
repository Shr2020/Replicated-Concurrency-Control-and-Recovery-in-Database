import Transaction as tr
import TransactionStates as trst
import DataManager as dm

class TransactionManager:
    def __init__(self):
        self.time = 0
        self.sites = {}
        self.vars = []
        self.current_transactions = {}
        self.transaction_wait_queue = []
        self.deadlocked_transaction  = []
        self.all_transactions = {}
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
        pass

    def read_operation(self, transaction_id, variable):
        pass

    def dump(self):
        pass

    def end_transaction(self, transaction_id):
        pass

    def abort_transaction(self, transaction_id):
        pass

    def fail_site(site):
        site_obj = sites[site]
        site_obj.fail_site(site)

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
            self.write_operation(t_id, var, val)

        elif op == "R":
            t_id = op[1]
            var = op[2]
            self.read_operation(t_id, var)
        
        elif op == "dump":
            self.dump()

        elif op == "fail":
            site = op[1]
            self.fail_site(site)

        elif op == "recover":
            site = op[1]
            self.recover_site(site)
            


