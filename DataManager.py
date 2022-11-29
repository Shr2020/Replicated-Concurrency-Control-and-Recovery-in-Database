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
        self.initialize_site_vars(id)
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
    def initialize_site_vars(self, id):
        listt = []
        for i in range (1, 21):
            if (i % 2 == 0):
                listt.append("x" + str(i))
            else:
                if (id == 1 + (i % 10)):
                    listt.append("x" + str(i))
        self.variables = set(listt)

    def is_var_in_site(self, var):
        return var in self.variables;
        
    def release_lock(self, var):
        pass

    def acquire_lock(self, var):
        pass

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