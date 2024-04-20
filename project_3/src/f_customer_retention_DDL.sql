-- DROP TABLE IF EXISTS mart.f_customer_retention;
CREATE TABLE mart.f_customer_retention (
	new_customers_count int8 NULL,
	returning_customers_count int8 NULL,
	refunded_customer_count int8 NULL,
	period_name text NULL,
	period_id int4 NULL,
	item_id int4 NULL,
	new_customers_revenue numeric NULL,
	returning_customers_revenue numeric NULL,
	customers_refunded int8 NULL
);