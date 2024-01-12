

class DatabaseUtilitiesHandler:    
    """
    Singleton class to handle database backend connections.
    Pure backend manager component to get rid of dependency hell.
    """
    _instance = None
  
    def __init__(self):
        self.db_connections = []
        self.last_edited_database_uid = None
        self.last_active_database = None
        self.database_uid_db_connection_dict = {}

    def __new__(cls, *args, **kwargs):
        """Ensure it is a singleton"""
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls)
        return cls._instance

    # DB Connections
    # TODO: IT IS POSSIBLE TO HAVE DUPLICATE DB NAMES
    def new_database_connection(self, db_connection):
        # TODO: should check whether db connection is valid and raise exception if not -> Dominik: I dont agree
        #assert db_connection.is_valid_db_connection
        self.db_connections.append(db_connection)


    def get_selected_db_connection(self, selected_db_name):
        valid_dbs = {x.database + " (" + x.server + ")": x for x in self.db_connections if hasattr(x, "database")}
        db_connection = valid_dbs.get(selected_db_name, None)

        return db_connection

    def get_db_connection(self, db_name):
        for connection in self.db_connections:
            if connection.database == db_name:
                return connection
            
    def remove_db_connection_by_database_uid(self, database_uid: str):
        db_connection = self.database_uid_db_connection_dict.pop(database_uid)
        self.db_connections.remove(db_connection)

duh = DatabaseUtilitiesHandler()