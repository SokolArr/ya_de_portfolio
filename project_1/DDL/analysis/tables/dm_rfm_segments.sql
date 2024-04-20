CREATE TABLE analysis.dm_rfm_segments (
    user_id int4 UNIQUE NOT NULL,
    recency int4 NOT NULL CHECK(recency >= 1 AND recency <= 5),
    frequency int4 NOT NULL CHECK(frequency >= 1 AND frequency <= 5),  
    monetary_value int4 NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);