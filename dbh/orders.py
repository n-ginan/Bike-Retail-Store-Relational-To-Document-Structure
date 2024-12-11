from helper import Helper
from pymongo import UpdateOne

class Orders(Helper):

    __collection = Helper._initialize_db()["orders"]

    @staticmethod
    def get_collection():

        return Orders.__collection

    @staticmethod
    def get_orders():

        return Orders.get_collection().find().to_list()
    
    @staticmethod
    def embed_orders():

        data = Orders.get_orders()
        bulk = []

        for datum in data:
            updated_data = {
                "$unset": {
                    "order_date": datum["order_date"],
                    "required_date": datum["required_date"],
                    "shipped_date": datum["shipped_date"]
                },
                "$set": {
                    "dates": {
                        "order_date": datum["order_date"],
                        "required_date": datum["required_date"],
                        "shipped_date": datum["shipped_date"]
                    }
                }
            }
            bulk.append(UpdateOne(
                {"_id": datum["_id"]},
                updated_data
            ))

        return Orders.get_collection().bulk_write(bulk).modified_count
    
if __name__ == "__main__":
    print(Orders.embed_orders())