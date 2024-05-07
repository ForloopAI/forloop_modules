from typing import Literal, Optional, Union

from pydantic import BaseModel

import dbhydra.dbhydra_core as dh
import forloop_modules.flog as flog
from forloop_modules.redis.redis_connection import (
    create_redis_key_for_project_db_private_key,
    kv_redis,
)
from forloop_modules.utils.encryption import (
    convert_base64_private_key_to_rsa_private_key,
    decrypt_text,
)

DbDialect = Literal["MySQL", "SQL Server", "PostgreSQL", "Xlsx structure", "MongoDB", "BigQuery"]
DBInstance = Union[dh.MysqlDb, dh.SqlServerDb, dh.PostgresDb, dh.XlsxDB, dh.MongoDb, dh.BigQueryDb]


class DbDetails(BaseModel):
    DB_SERVER: str
    DB_PASSWORD: str
    DB_PORT: int
    DB_USERNAME: str
    DB_DATABASE: str
    LOCALLY: bool
    DIALECT: DbDialect

    def __getitem__(self, key):
        return getattr(self, key)


class DbConnection:
    def __init__(self, db_details: Union[DbDetails, dict], is_stored: bool = False):
        self.db_details = db_details.model_dump(
        ) if isinstance(db_details, DbDetails) else db_details
        self.server = db_details["DB_SERVER"]
        self.database = db_details["DB_DATABASE"]
        self.is_stored = is_stored
        self.is_valid_db_connection: bool = False

        # Assigned after testing the connection
        self.db_instance: Optional[DBInstance] = None
        self.is_connected: bool = False
        self.table_dict: Optional[dict] = None
        self.foreign_keys: Optional[list] = None

        # TODO: This should not be here. Examine the code and delete it everywhere.
        self.images = []  # icons covering this DbConnection

    def create_new_db(self):
        if self.db_details["DIALECT"] == "MySQL":
            dh.MysqlDb(db_details=self.db_details).create_new_db()
            return True
        return False

    # def _store_db_details_if_valid(self):
    #     if not self.is_stored and self.is_valid_db_connection:
    #         db_details = self.db_details
    #         db_details_list = kv.load_variable_safe("db_details.kpv", "db_details_list")
    #         print("DB_DETAILS", db_details)
    #         db_details_list.append(db_details)
    #         db_details_list = kv.VarSafe(db_details_list, "db_details_list", "db_details_list")
    #         kv.save_variables(kv.kept_variables, "db_details.kpv")
    #         self.is_stored = True

    def _test_connection_dbhydra_db(self):
        try:
            if self.db_details["DIALECT"] == "MySQL":
                self.db_instance = dh.MysqlDb(db_details=self.db_details)
                # do not delete - checks whether the connection is valid
                with self.db_instance.connect_to_db():
                    pass
            elif self.db_details["DIALECT"] == "SQL Server":
                self.db_instance = dh.SqlServerDb(db_details=self.db_details)
                # do not delete - checks whether the connection is valid
                with self.db_instance.connect_to_db():
                    pass
            elif self.db_details["DIALECT"] == "PostgreSQL":
                self.db_instance = dh.PostgresDb(db_details=self.db_details)
                with self.db_instance.connect_to_db():
                    pass
            elif self.db_details["DIALECT"] == "Xlsx structure":
                self.db_instance = dh.XlsxDb()  # db_details=self.db_details)
                # TODO: missing db_instance.connect()
                self.db_instance.close_connection()  # TODO: Xlsx DB
            elif self.db_details["DIALECT"] == "MongoDB":
                self.db_instance = dh.MongoDb(db_details=self.db_details)
                with self.db_instance.connect_to_db():
                    pass
            elif self.db_details["DIALECT"] == "BigQuery":
                self.db_instance = dh.BigQueryDb(db_details=self.db_details)
                with self.db_instance.connect_to_db():
                    pass
            else:
                return False

            self.is_valid_db_connection = True
            # self._store_db_details_if_valid()
            return True
        except Exception:
            self.db_instance = None
            self.is_valid_db_connection = False
            flog.error("Database Connection Failed!")

            return False

    def test_database_connection(self) -> bool:
        """
        Test connection to database
        if connected successfully, get database tables.
        """
        is_connected = self._test_connection_dbhydra_db()

        if is_connected:
            self.get_table_dict_and_fk()

        return is_connected

    def get_table_dict_and_fk(self):
        with self.db_instance.connect_to_db():
            self.table_dict = self.db_instance.generate_table_dict()
            if self.db_details["DIALECT"] == "SQL Server":
                self.foreign_keys = self.db_instance.get_foreign_keys_columns()
            else:
                self.foreign_keys = []
        return (self.table_dict, self.foreign_keys)

    def get_db_table(self, db_table_name: str):
        if self.table_dict is None:
            return

        for table_name, table in self.table_dict.items():
            if table_name == db_table_name:
                return table


def create_db_details_from_database_dict(db_dict: dict):
    db_details = DbDetails(
        DB_SERVER=db_dict["server"],
        DB_PASSWORD=db_dict["password"],
        DB_PORT=db_dict["port"],
        DB_USERNAME=db_dict["username"],
        DB_DATABASE=db_dict["database"],
        LOCALLY=False,
        DIALECT=db_dict["dialect"],
    )

    return db_details


def decrypt_db_details(database: dict) -> DbDetails:
    """Decrypt the password of the database using the private key stored in Redis."""
    redis_key = create_redis_key_for_project_db_private_key(project_uid=database['project_uid'])
    private_key_base64 = kv_redis.get(redis_key)

    if private_key_base64 is not None:
        private_key = convert_base64_private_key_to_rsa_private_key(
            private_key_base64=private_key_base64
        )

        encrypted_password = database["password"]
        decrypted_password = decrypt_text(text=encrypted_password, private_key=private_key)

        database["password"] = decrypted_password

        return create_db_details_from_database_dict(db_dict=database)
    else:
        raise ValueError(f"Private key not found for Project:{database['project_uid']} in Redis.")
