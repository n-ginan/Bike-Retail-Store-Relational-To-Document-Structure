from pymongo import MongoClient

class Helper:
    
    __client = None
    __database = None
    _instance_preventer = False

    def __new__(cls):
        if cls.__client is None:
            cls.__client = super(Helper, cls).__new__(cls)
        return cls.__client
    
    def __init__(self):
        if not Helper._instance_preventer:
            raise Exception("This class implements a singleton pattern, please call _initialize_db() instead.")
        Helper.__client = MongoClient("mongodb://localhost:26234/")
        Helper.__database = Helper.__client["bike_store_db"]

    @classmethod
    def _initialize_db(cls):
        if cls.__database is None:
            cls._instance_preventer = True
            Helper()
        return cls.__database
    

if __name__ == "__main__":
    pass