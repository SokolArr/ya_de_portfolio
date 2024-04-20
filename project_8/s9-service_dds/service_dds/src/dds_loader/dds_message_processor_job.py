import json
from logging import Logger
from typing import List, Dict
from datetime import datetime

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from dds_loader.repository import DdsRepository, OrderDdsBuilder

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"message pocessor start")
        cnt = 0
        
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.info(f"NO messages. Quitting.")
                break
            cnt = cnt + 1
            self._logger.info(f"pocess {cnt} message")
            
            b = OrderDdsBuilder(msg['payload'])
            
            self.load_hubs(b)
            self.load_links(b)
            self.load_sats(b)
            
            self._producer.produce(self.make_send_message(b))
            
        self._logger.info(f"finally {cnt} message pocessed")
        
        
    def load_hubs(self, b: OrderDdsBuilder) -> None:
        self._dds_repository.h_user_insert(b.h_user())
        self._dds_repository.h_restaurant_insert(b.h_restaurant())
        self._dds_repository.h_order_insert(b.h_order())
        
        for product in b.h_product().array:
            self._dds_repository.h_product_insert(product)
            
        for category in b.h_category().array:
            self._dds_repository.h_category_insert(category)
        
    def load_links(self, b: OrderDdsBuilder) -> None:
        self._dds_repository.l_order_user_insert(b.l_order_user())
        
        for order_product in b.l_order_product().array:
            self._dds_repository.l_order_product_insert(order_product)
            
        for product_category in b.l_product_category().array:
            self._dds_repository.l_product_category_insert(product_category)
            
        for product_restaurant in b.l_product_restaurant().array:
            self._dds_repository.l_product_restaurant_insert(product_restaurant)
            
    def load_sats(self, b: OrderDdsBuilder) -> None:
        self._dds_repository.s_order_cost_insert(b.s_order_cost())
        self._dds_repository.s_order_status_insert(b.s_order_status())
        self._dds_repository.s_restaurant_names_insert(b.s_restaurant_names())
        self._dds_repository.s_user_names_insert(b.s_user_names())
        
        for product_name in b.s_product_names().array:
            self._dds_repository.s_product_names_insert(product_name)
            
    def make_send_message(self, b: OrderDdsBuilder) -> dict:
        return {
                "object_id": b.h_order().h_order_pk,
                "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "object_type": "order_report",
                "payload": {
                    "id": b.h_order().h_order_pk,
                    "order_dt": b.h_order().date,
                    "status": b.s_order_status().status,
                    "restaurant": {
                        "id": b.h_restaurant().h_restaurant_pk,
                        "name": b.s_restaurant_names().name
                    },
                    "user": {
                        "id": b.h_user().h_user_pk,
                        "username": b.s_user_names().username
                    },
                    "products": b.get_list_of_products()
                }
        }