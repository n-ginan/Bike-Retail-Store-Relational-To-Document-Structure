from helper import Helper
from pymongo import UpdateOne

class Stores(Helper):

    __collection = Helper._initialize_db()["stores"]

    @staticmethod
    def get_collection():

        return Stores.__collection

    @staticmethod
    def get_stores():

        return Stores.get_collection().find().to_list()
    
    @staticmethod
    def embed_stores():
        
        data = Stores.get_stores()
        bulk = []

        for datum in data:
            updated_data = {
                "$unset": {
                    "phone": datum["phone"],
                    "email": datum["email"],
                    "street": datum["street"],
                    "city": datum["city"],
                    "state": datum["state"],
                    "zip_code": datum["zip_code"],
                },
                "$set": {
                    "store_contacts": {
                        "phone": datum["phone"],
                        "email": datum["email"]
                    },
                    "store_address": {
                        "street": datum["street"],
                        "city": datum["city"],
                        "state": datum["state"],
                        "zip_code": datum["zip_code"],
                    }
                }
            }

            bulk.append(UpdateOne(
                {"_id": datum["_id"]},
                updated_data
            ))

        return Stores.get_collection().bulk_write(bulk).modified_count
    
if __name__ == "__main__":
    print(Stores.embed_stores())