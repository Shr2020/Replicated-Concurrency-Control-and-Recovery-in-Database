import DB
class DataManager:
    def __init__(self, id):
        self.db = DB()
        self.is_available = True
        self.lock_map = {}
        self.site_id = id
        self.buffer = {}
        self.backups = {}

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