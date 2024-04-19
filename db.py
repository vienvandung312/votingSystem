import psycopg2 as pg


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SingletonDBConnection(metaclass=SingletonMeta):

    def __init__(self, host: str, dbname: str, user: str, password: str):
        """
        Singleton implementation of the Postgres connection
        """
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    def connect(self):
        try:
            conn = pg.connect("host={} dbname={} user={} password={}"
                              .format(self.host, self.dbname, self.user, self.password))
            return conn
        except Exception as e:
            raise e