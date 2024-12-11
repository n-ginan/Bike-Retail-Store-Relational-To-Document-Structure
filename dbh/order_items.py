from helper import Helper
from pymongo import UpdateOne

class OrderItemsCollection(Helper):

    __collection = Helper._initialize_db()["order_items"]

    @staticmethod
    def get_collection():
        return OrderItemsCollection.__collection

    @staticmethod
    def get_order_items():
        return OrderItemsCollection.get_collection().find().to_list()
    
    @staticmethod
    def embed_order_price():

        data = OrderItemsCollection.get_order_items()
        bulk = []

        for datum in data:
            update_datum = {
                "$unset": {
                    "list_price": datum["list_price"],
                    "discount": datum["discount"]
                },
                "$set": {
                    "price": {
                        "list_price": datum["list_price"],
                        "discount": datum["discount"]
                    }
                }
            }
            bulk.append(UpdateOne(
                {"_id": datum["_id"]},
                update_datum
            ))
        
        return OrderItemsCollection.get_collection().bulk_write(bulk).modified_count

if __name__ == "__main__":
    print(OrderItemsCollection.embed_order_price())