import json
from logging import Logger
from datetime import datetime

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from cdm_loader.repository import CdmRepository, OrderCdmBuilder

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 dds_repository: CdmRepository,
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
            self._logger.info(f"{msg}")
            
            b = OrderCdmBuilder(msg['payload'])
            
            self.load_cdm(b)
            
        self._logger.info(f"finally {cnt} message pocessed")
        
        
    def load_cdm(self, b: OrderCdmBuilder) -> None:
        for product in b.user_category_counters().array:
            self._dds_repository.user_category_counters_insert(product)
            
        for product in b.user_product_counters().array:
            self._dds_repository.user_product_counters_insert(product)