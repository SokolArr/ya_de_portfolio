import uuid
from datetime import datetime


class User():
    def __init__(self, order_dict: dict):
        self.user_id = order_dict['user']['id']

class Product():
     def __init__(self, product_dict: dict):
        self.product_id = product_dict['id']
        self.product_name = product_dict['name']
        self.category_id = product_dict['category']['id']
        self.category_name = product_dict['category']['name']

class Products(Product):
     def __init__(self, order_dict: dict):
        self.array = []
        for product_dict in order_dict['products']:
            self.array.append(Product(product_dict))
    
class UserCategory(User):
    def __init__(self, order_dict: dict, product: Product):
        self.user_id = User(order_dict).user_id
        self.category_id = product.category_id
        self.category_name = product.category_name

class UserCategories(UserCategory):
    def __init__(self, order_dict: dict):
        self.array = []
        for product in Products(order_dict).array:
            self.array.append(UserCategory(order_dict, product))
            
class UserProduct(Product):
    def __init__(self, order_dict: dict, product: Product):
        self.user_id = User(order_dict).user_id
        self.product_id = product.product_id
        self.product_name = product.product_name

class UserProducts():
    def __init__(self, order_dict: dict):
        self.array = []
        for product in Products(order_dict).array:
            self.array.append(UserProduct(order_dict, product))
            
class OrderCdmBuilder():
    def __init__(self, order_dict) -> dict:
        self._order_dict = order_dict
        
    def user_category_counters(self) -> UserCategories:
        return UserCategories(self._order_dict)
    
    def user_product_counters(self) -> UserProducts:
        return UserProducts(self._order_dict)