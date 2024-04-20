import uuid
from datetime import datetime

class User():
    def __init__(self, _order_dict):
        self.h_user_pk = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                        str(_order_dict['user']['id']))
        )
        self.id = _order_dict['user']['id']
        self.name = _order_dict['user']['name']
        self.login = _order_dict['user']['login']
        
class Product():
    def __init__(self, _product_dict):
        self.h_product_pk = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                           str(_product_dict['id']))
        )
        self.id = _product_dict['id']
        self.price = _product_dict['price']
        self.quantity = _product_dict['quantity']
        self.name = _product_dict['name']
        self.category = _product_dict['category']
        
class Products(Product):
    def __init__(self, _order_dict):
        self.array = []
        for product in _order_dict['products']:
            self.array.append(Product(product))
            
class Category(Product):
    def __init__(self, product: Product):
        self.h_category_pk = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                            str(product.category))
        )
        self.name = product.category
             
class Categories(Category, Products):
    def __init__(self, _order_dict):
        self.array = []
        for product in Products(_order_dict).array:
            self.array.append(Category(product))
            
class Restaurant():
    def __init__(self, _order_dict):
        self.h_restaurant_pk = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                              str(_order_dict['restaurant']['id']))
        )
        self.id = _order_dict['restaurant']['id']
        self.name = _order_dict['restaurant']['name']
        
class Order(Restaurant, User, Products):
    def __init__(self, _order_dict):
        self.h_order_pk = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                         str(_order_dict['id']))
        )
        self.id = _order_dict['id']
        self.date = _order_dict['date']
        self.cost = _order_dict['cost']
        self.payment = _order_dict['payment']
        self.status = _order_dict['status']
        self.restaurant = Restaurant(_order_dict)
        self.user = User(_order_dict)
        self.products = Products(_order_dict)
        
class OrderUser(Order):
    def __init__(self, order: Order):
        self.hk_order_user_pk = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                               order.h_order_pk +
                                               order.user.h_user_pk)
        )
        self.h_user_pk = order.user.h_user_pk
        self.h_order_pk = order.h_order_pk
        
class OrderProduct(Order, Product):
    def __init__(self, order: Order, product: Product):
        self.hk_order_product_pk = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                                  order.h_order_pk + 
                                                  product.h_product_pk)
                                        
        )
        self.h_order_pk = order.h_order_pk
        self.h_product_pk = product.h_product_pk
        
class OrderProducts():
    def __init__(self, order: Order):
        self.array = []
        for product in order.products.array:
            self.array.append(OrderProduct(order, product))

class ProductCategory(Category, Product):
    def __init__(self, product: Product):
        self.hk_product_category_pk = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                                  product.h_product_pk +
                                                  Category(product).h_category_pk)
        )
        self.h_product_pk = product.h_product_pk
        self.h_category_pk = Category(product).h_category_pk

class ProductsCategory():
    def __init__(self, order: Order):
        self.array = []
        for product in order.products.array:
            self.array.append(ProductCategory(product))

class ProductRestaurant(Order, Product):
    def __init__(self, order: Order, product: Product):
        self.hk_product_restaurant_pk = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                                 product.h_product_pk +
                                                 order.restaurant.h_restaurant_pk)
        )
        self.h_product_pk = product.h_product_pk
        self.h_restaurant_pk = order.restaurant.h_restaurant_pk
        
class ProductsRestaurant():
    def __init__(self, order: Order):
        self.array = []
        for product in order.products.array:
            self.array.append(ProductRestaurant(order, product))
            
class OrderCost():
    def __init__(self, order: Order):
        self.h_order_pk = order.h_order_pk
        self.cost = order.cost
        self.payment = order.payment
        self.hk_order_cost_hashdiff = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                                 order.h_order_pk + 
                                                 str(order.cost))
        )
        
class OrderStatus():
    def __init__(self, order: Order):
        self.h_order_pk = order.h_order_pk
        self.status = order.status
        self.hk_order_status_hashdiff = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                                 order.h_order_pk + 
                                                 order.status)
        ) 

class ProductName():
    def __init__(self, product: Product):
        self.h_product_pk = product.h_product_pk
        self.name = product.name
        self.hk_product_names_hashdiff = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                                 product.h_product_pk + 
                                                 product.name)
        )

class ProductsName():
    def __init__(self, order: Order):
        self.array = []
        for product in order.products.array:
            self.array.append(ProductName(product))
            
class RestaurantNames():
    def __init__(self, order: Order):
        self.h_restaurant_pk = order.restaurant.h_restaurant_pk
        self.name = order.restaurant.name
        self.hk_restaurant_names_hashdiff = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                                 order.restaurant.h_restaurant_pk + 
                                                 order.restaurant.name)
        )

class UserNames():
    def __init__(self, order: Order):
        self.h_user_pk = order.user.h_user_pk
        self.username = order.user.name
        self.userlogin = order.user.login
        self.hk_user_names_hashdiff = str(uuid.uuid3(uuid.NAMESPACE_DNS, 
                                                 order.user.h_user_pk + 
                                                 order.user.name)
        )
    
class OrderDdsBuilder():
    def __init__(self, order_dict) -> dict:
        self._order_dict = order_dict
    
    def h_user(self) -> User:
        return User(self._order_dict)
    
    def h_product(self) -> Products:
        return Products(self._order_dict)
    
    def h_category(self) -> Categories:
        return Categories(self._order_dict)
    
    def h_restaurant(self) -> Restaurant:
        return Restaurant(self._order_dict)
    
    def h_order(self) -> Order:
        return Order(self._order_dict)
    
    def l_order_product(self) -> OrderProducts:
        return OrderProducts(Order(self._order_dict))
    
    def l_order_user(self) -> OrderUser:
        return OrderUser(Order(self._order_dict))
    
    def l_product_category(self) -> ProductsCategory:
        return ProductsCategory(Order(self._order_dict))
    
    def l_product_restaurant(self) -> ProductsRestaurant:
        return ProductsRestaurant(Order(self._order_dict))
    
    def s_order_cost(self) -> OrderCost:
        return OrderCost(Order(self._order_dict))
    
    def s_order_status(self) -> OrderStatus:
        return OrderStatus(Order(self._order_dict))
    
    def s_product_names(self) -> ProductsName:
        return ProductsName(Order(self._order_dict))
    
    def s_restaurant_names(self) -> RestaurantNames:
        return RestaurantNames(Order(self._order_dict))
    
    def s_user_names(self) -> UserNames:
        return UserNames(Order(self._order_dict))
    
    def get_list_of_products(self) -> list:
        array = []
        for product in Order(self._order_dict).products.array:
            array.append({
                'id': product.h_product_pk,
                'name': product.name,
                'category': {
                    'id': Category(product).h_category_pk,
                    'name': Category(product).name
                }     
            })
        return array