from helper import Helper
from pymongo import ReplaceOne

class CustomersCollection(Helper):

    __collection = Helper._initialize_db()["customers"]

    @staticmethod
    def get_collection():
        return CustomersCollection.__collection

    @staticmethod
    def get_customers():

        return CustomersCollection.get_collection().find().to_list()
    
    @staticmethod # Used for cleaning documents
    def embed_customer_names():

        data = CustomersCollection.get_customers()
        bulk = []

        for datum in data:
            replace_with = {
                "customer_id": datum["customer_id"],
                "full_name": {
                    "first_name": datum["first_name"],
                    "last_name": datum["last_name"]
                },
                "contacts": {
                    "phone": datum["phone"],
                    "email": datum["email"]
                },
                "address": {
                    "street": datum["street"],
                    "city": datum["city"],
                    "state": datum["state"],
                    "zip_code": datum["zip_code"]
                }
            }
            bulk.append(ReplaceOne(
                {"customer_id": datum["customer_id"]}, replace_with
            ))

        return CustomersCollection.get_collection().bulk_write(bulk).modified_count



if __name__ == "__main__":
    print(CustomersCollection.embed_customer_names())