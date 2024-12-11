from helper import Helper
from pymongo import UpdateOne

class Staffs(Helper):

    __collection = Helper._initialize_db()["staffs"]

    @staticmethod
    def get_collection():

        return Staffs.__collection

    @staticmethod
    def get_staffs():

        return Staffs.get_collection().find().to_list()
    
    @staticmethod
    def embed_staff():
        
        data = Staffs.get_staffs()
        bulk = []

        for datum in data:
            update_data = {
                "$unset": {
                    "first_name": datum["first_name"],
                    "last_name": datum["last_name"],
                    "email": datum["email"],
                    "phone": datum["phone"]
                },
                "$set": {
                    "full_name": {
                        "first_name": datum["first_name"],
                        "last_name": datum["last_name"]
                    },
                    "contacts": {
                        "email": datum["email"],
                        "phone": datum["phone"]
                    }
                }
            }

            bulk.append(UpdateOne(
                {"_id": datum["_id"]},
                update_data
            ))

        return Staffs.get_collection().bulk_write(bulk)
    
if __name__ == "__main__":
    Staffs.embed_staff()