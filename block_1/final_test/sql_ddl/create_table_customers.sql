CREATE TABLE IF NOT EXISTS public.customers
(
    customer_id character varying COLLATE pg_catalog."default" NOT NULL,
    "FN" numeric,
    "Active" numeric,
    club_member_status character varying COLLATE pg_catalog."default",
    fashion_news_frequency character varying COLLATE pg_catalog."default",
    age integer,
    postal_code character varying COLLATE pg_catalog."default",
    CONSTRAINT customers_pkey PRIMARY KEY (customer_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.customers
    OWNER to postgres;