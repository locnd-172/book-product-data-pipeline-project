create_staging_book_product_id_table='''
    CREATE TABLE IF NOT EXISTS staging.book_product_id (
        product_id CHARACTER VARYING NOT NULL PRIMARY KEY
    );
'''

create_staging_book_product_data_table='''
    CREATE TABLE IF NOT EXISTS staging.book_product_data (
        id bigserial NOT NULL,
        product_id character varying NOT NULL,
        name character varying NOT NULL,
        sku character varying NOT NULL,
        price double precision NOT NULL,
        original_price double precision,
        discount double precision,
        discount_rate double precision,
        image_url text,
        author character varying,
        quantity_sold integer,
        publisher character varying,
        manufacturer character varying,
        number_of_pages character varying,
        translator character varying,
        publication_date date default NULL,
        book_cover character varying,
        width real,
        height real,
        category character varying,
        category_id integer,
        PRIMARY KEY (id),
        CONSTRAINT product_id_unique UNIQUE (product_id)
    );
'''
create_staging_book_product_review_table='''
    CREATE TABLE IF NOT EXISTS staging.book_product_review (
        id bigserial NOT NULL,
        product_id character varying NOT NULL, -- REFERENCES staging.book_product(product_id),
        rating_average real DEFAULT 0,
        reviews_count integer DEFAULT 0,
        count_1_star integer DEFAULT 0,
        percent_1_star real DEFAULT 0,
        count_2_star integer DEFAULT 0,
        percent_2_star real DEFAULT 0,
        count_3_star integer DEFAULT 0,
        percent_3_star real DEFAULT 0,
        count_4_star integer DEFAULT 0,
        percent_4_star real DEFAULT 0,
        count_5_star integer DEFAULT 0,
        percent_5_star real DEFAULT 0,
        PRIMARY KEY (id),
        CONSTRAINT product_id_unique_rv UNIQUE (product_id)
    );
'''