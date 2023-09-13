CREATE TABLE IF NOT EXISTS public.transactions_train
(
    t_dat date,
    customer_id character varying COLLATE pg_catalog."default",
    article_id integer,
    price numeric,
    sales_channel_id integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.transactions_train
    OWNER to postgres;