from datetime import datetime
from lib.pg import PgConnect
from .order_dds_builder import *

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        
    def h_user_insert(self,
                      user: User
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.h_user(
                        h_user_pk,
                        user_id,
                        load_dt,
                        load_src
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4
                    from(
                        values(
                            %(h_user_pk)s,
                            %(user_id)s,
                            current_timestamp,
                            'kafka'
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_user_pk from dds.h_user)
                """,
                    {   
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.id
                    }
                )
                
    def h_restaurant_insert(self,
                      restaurant: Restaurant
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.h_restaurant(
                        h_restaurant_pk,
                        restaurant_id,
                        load_dt,
                        load_src
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4
                    from(
                        values(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            current_timestamp,
                            'kafka'
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_restaurant_pk from dds.h_restaurant)
                """,
                    {   
                        'h_restaurant_pk': restaurant.h_restaurant_pk,
                        'restaurant_id': restaurant.id
                    }
                )
                
    def h_order_insert(self,
                      order: Order
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.h_order(
                        h_order_pk,
                        order_id,
                        load_dt,
                        load_src,
                        order_dt
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4,
                        column5
                    from(
                        values(
                            %(h_order_pk)s,
                            %(order_id)s,
                            current_timestamp,
                            'kafka',
                            %(order_dt)s::timestamp
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_order_pk from dds.h_order)
                """,
                    {   
                        'h_order_pk': order.h_order_pk,
                        'order_id': order.id,
                        'order_dt': order.date
                    }
                )
                
    def h_product_insert(self,
                      product: Product
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.h_product(
                        h_product_pk,
                        product_id,
                        load_dt,
                        load_src
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4
                    from(
                        values(
                            %(h_product_pk)s,
                            %(product_id)s,
                            current_timestamp,
                            'kafka'
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_product_pk from dds.h_product)
                """,
                    {   
                        'h_product_pk': product.h_product_pk,
                        'product_id': product.id
                    }
                )
                
    def h_category_insert(self,
                      category: Category
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.h_category(
                        h_category_pk,
                        category_name,
                        load_dt,
                        load_src
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4
                    from(
                        values(
                            %(h_category_pk)s,
                            %(category_name)s,
                            current_timestamp,
                            'kafka'
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_category_pk from dds.h_category)
                """,
                    {   
                        'h_category_pk': category.h_category_pk,
                        'category_name': category.name
                    }
                )
 
    def l_order_user_insert(self,
                      order_user: OrderUser
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.l_order_user(
                        hk_order_user_pk,
                        h_user_pk,
                        h_order_pk,
                        load_dt,
                        load_src
                    )
                    select 
                        column1::uuid,
                        column2::uuid,
                        column3::uuid,
                        column4,
                        column5
                    from(
                        values(
                            %(hk_order_user_pk)s,
                            %(h_user_pk)s,
                            %(h_order_pk)s,
                            current_timestamp,
                            'kafka'
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct hk_order_user_pk from dds.l_order_user)
                """,
                    {   
                        'hk_order_user_pk': order_user.hk_order_user_pk,
                        'h_user_pk': order_user.h_user_pk,
                        'h_order_pk': order_user.h_order_pk
                    }
                )
    
    def l_order_product_insert(self,
                      order_product: OrderProduct
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.l_order_product(
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    select 
                        column1::uuid,
                        column2::uuid,
                        column3::uuid,
                        column4,
                        column5
                    from(
                        values(
                            %(hk_order_product_pk)s,
                            %(h_order_pk)s,
                            %(h_product_pk)s,
                            current_timestamp,
                            'kafka'
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct hk_order_product_pk from dds.l_order_product)
                """,
                    {   
                        'hk_order_product_pk': order_product.hk_order_product_pk,
                        'h_order_pk': order_product.h_order_pk,
                        'h_product_pk': order_product.h_product_pk
                    }
                )
                
    def l_product_category_insert(self,
                      product_category: ProductCategory
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.l_product_category(
                        hk_product_category_pk,
                        h_category_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    select 
                        column1::uuid,
                        column2::uuid,
                        column3::uuid,
                        column4,
                        column5
                    from(
                        values(
                            %(hk_product_category_pk)s,
                            %(h_category_pk)s,
                            %(h_product_pk)s,
                            current_timestamp,
                            'kafka'
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct hk_product_category_pk from dds.l_product_category)
                """,
                    {   
                        'hk_product_category_pk': product_category.hk_product_category_pk,
                        'h_category_pk': product_category.h_category_pk,
                        'h_product_pk': product_category.h_product_pk
                    }
                )
                
    def l_product_restaurant_insert(self,
                      product_restaurant: ProductRestaurant
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.l_product_restaurant(
                        hk_product_restaurant_pk,
                        h_restaurant_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    select 
                        column1::uuid,
                        column2::uuid,
                        column3::uuid,
                        column4,
                        column5
                    from(
                        values(
                            %(hk_product_restaurant_pk)s,
                            %(h_restaurant_pk)s,
                            %(h_product_pk)s,
                            current_timestamp,
                            'kafka'
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct hk_product_restaurant_pk from dds.l_product_restaurant)
                """,
                    {   
                        'hk_product_restaurant_pk': product_restaurant.hk_product_restaurant_pk,
                        'h_restaurant_pk': product_restaurant.h_restaurant_pk,
                        'h_product_pk': product_restaurant.h_product_pk
                    }
                )
                
    def s_order_cost_insert(self,
                      order_cost: OrderCost
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.s_order_cost(
                        h_order_pk,
                        cost,
                        payment,
                        load_dt,
                        load_src,
                        hk_order_cost_hashdiff
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4,
                        column5,
                        column6::uuid
                    from(
                        values(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            current_timestamp,
                            'kafka',
                            %(hk_order_cost_hashdiff)s
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_order_pk from dds.s_order_cost)
                """,
                    {   
                        'h_order_pk': order_cost.h_order_pk,
                        'cost': order_cost.cost,
                        'payment': order_cost.payment,
                        'hk_order_cost_hashdiff': order_cost.hk_order_cost_hashdiff
                    }
                )
                
    def s_order_status_insert(self,
                      order_status: OrderStatus
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.s_order_status(
                        h_order_pk,
                        status,
                        load_dt,
                        load_src,
                        hk_order_status_hashdiff
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4,
                        column5::uuid
                    from(
                        values(
                            %(h_order_pk)s,
                            %(status)s,
                            current_timestamp,
                            'kafka',
                            %(hk_order_status_hashdiff)s
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_order_pk from dds.s_order_status)
                """,
                    {   
                        'h_order_pk': order_status.h_order_pk,
                        'status': order_status.status,
                        'hk_order_status_hashdiff': order_status.hk_order_status_hashdiff
                    }
                )
    
    def s_restaurant_names_insert(self,
                      restaurant_names: RestaurantNames
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.s_restaurant_names(
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_restaurant_names_hashdiff
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4,
                        column5::uuid
                    from(
                        values(
                            %(h_restaurant_pk)s,
                            %(name)s,
                            current_timestamp,
                            'kafka',
                            %(hk_restaurant_names_hashdiff)s
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_restaurant_pk from dds.s_restaurant_names)
                """,
                    {   
                        'h_restaurant_pk': restaurant_names.h_restaurant_pk,
                        'name': restaurant_names.name,
                        'hk_restaurant_names_hashdiff': restaurant_names.hk_restaurant_names_hashdiff
                    }
                )
    
    def s_user_names_insert(self,
                      user_names: UserNames
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.s_user_names(
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src,
                        hk_user_names_hashdiff
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4,
                        column5,
                        column6::uuid
                    from(
                        values(
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            current_timestamp,
                            'kafka',
                            %(hk_user_names_hashdiff)s
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_user_pk from dds.s_user_names)
                """,
                    {   
                        'h_user_pk': user_names.h_user_pk,
                        'username': user_names.username,
                        'userlogin': user_names.userlogin,
                        'hk_user_names_hashdiff': user_names.hk_user_names_hashdiff
                    }
                )
                
    def s_product_names_insert(self,
                      product_name: ProductName
    ) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    insert into dds.s_product_names(
                        h_product_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_product_names_hashdiff
                    )
                    select 
                        column1::uuid,
                        column2,
                        column3,
                        column4,
                        column5::uuid
                    from(
                        values(
                            %(h_product_pk)s,
                            %(name)s,
                            current_timestamp,
                            'kafka',
                            %(hk_product_names_hashdiff)s
                        )
                    )ins_mes
                    where column1::uuid not in (select distinct h_product_pk from dds.s_product_names)
                """,
                    {   
                        'h_product_pk': product_name.h_product_pk,
                        'name': product_name.name,
                        'hk_product_names_hashdiff': product_name.hk_product_names_hashdiff
                    }
                )