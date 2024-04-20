from datetime import datetime
from lib.pg import PgConnect
from .order_cdm_builder import *

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        
    def user_category_counters_insert(self,
                      user_category: UserCategory
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into cdm.user_category_counters(
                        user_id,
                        category_id,
                        category_name,
                        order_cnt
                    )
                    select 
                        column1::uuid,
                        column2::uuid,
                        column3,
                        column4
                    from(
                        values(
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            1
                        )
                    )ins_mes
                    on conflict (user_id, category_id) do update set
                            order_cnt = user_category_counters.order_cnt + 1,
                            category_name = excluded.category_name
                """,
                    {   
                        'user_id': user_category.user_id,
                        'category_id': user_category.category_id,
                        'category_name': user_category.category_name
                    }
                )
                
    def user_product_counters_insert(self,
                      user_product: UserProduct
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into cdm.user_product_counters(
                        user_id,
                        product_id,
                        product_name,
                        order_cnt
                    )
                    select 
                        column1::uuid,
                        column2::uuid,
                        column3,
                        column4
                    from(
                        values(
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            1
                        )
                    )ins_mes
                    on conflict (user_id, product_id) do update set
                            order_cnt = user_product_counters.order_cnt + 1,
                            product_name = excluded.product_name
                """,
                    {   
                        'user_id': user_product.user_id,
                        'product_id': user_product.product_id,
                        'product_name': user_product.product_name
                    }
                )
                