CREATE TABLE IF NOT EXISTS public.articles
(
    article_id integer NOT NULL,
    product_code integer,
    prod_name character varying COLLATE pg_catalog."default",
    product_type_no integer,
    product_type_name character varying COLLATE pg_catalog."default",
    product_group_name character varying COLLATE pg_catalog."default",
    graphical_appearance_no integer,
    graphical_appearance_name character varying COLLATE pg_catalog."default",
    colour_group_code integer,
    colour_group_name character varying COLLATE pg_catalog."default",
    perceived_colour_value_id integer,
    perceived_colour_value_name character varying COLLATE pg_catalog."default",
    perceived_colour_master_id integer,
    perceived_colour_master_name character varying COLLATE pg_catalog."default",
    department_no integer,
    department_name character varying COLLATE pg_catalog."default",
    index_code character varying COLLATE pg_catalog."default",
    index_name character varying COLLATE pg_catalog."default",
    index_group_no integer,
    index_group_name character varying COLLATE pg_catalog."default",
    section_no integer,
    section_name character varying COLLATE pg_catalog."default",
    garment_group_no integer,
    garment_group_name character varying COLLATE pg_catalog."default",
    detail_desc text COLLATE pg_catalog."default",
    CONSTRAINT articles_pkey PRIMARY KEY (article_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.articles
    OWNER to postgres;