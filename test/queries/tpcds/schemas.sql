
CREATE TABLE date_dim
(
    d_date_sk                 integer,
    d_date_id                 char(16),
    d_date                    date,
    d_month_seq               integer,
    d_week_seq                integer,
    d_quarter_seq             integer,
    d_year                    integer,
    d_dow                     integer,
    d_moy                     integer,
    d_dom                     integer,
    d_qoy                     integer,
    d_fy_year                 integer,
    d_fy_quarter_seq          integer,
    d_fy_week_seq             integer,
    d_day_name                char(9),
    d_quarter_name            char(6),
    d_holiday                 char(1),
    d_weekend                 char(1),
    d_following_holiday       char(1),
    d_first_dom               integer,
    d_last_dom                integer,
    d_same_day_ly             integer,
    d_same_day_lq             integer,
    d_current_day             char(1),
    d_current_week            char(1),
    d_current_month           char(1),
    d_current_quarter         char(1),
    d_current_year            char(1)
)
FROM FILE '../dbtoaster-experiments-data/tpcds/standard/data_dim.dat'
LINE DELIMITED CSV (delimiter := '|');


CREATE STREAM store_sales
(
    ss_sold_date_sk           integer,
    ss_sold_time_sk           integer,
    ss_item_sk                integer,
    ss_customer_sk            integer,
    ss_cdemo_sk               integer,
    ss_hdemo_sk               integer,
    ss_addr_sk                integer,
    ss_store_sk               integer,
    ss_promo_sk               integer,
    ss_ticket_number          integer,
    ss_quantity               integer,
    ss_wholesale_cost         decimal(7,2),
    ss_list_price             decimal(7,2),
    ss_sales_price            decimal(7,2),
    ss_ext_discount_amt       decimal(7,2),
    ss_ext_sales_price        decimal(7,2),
    ss_ext_wholesale_cost     decimal(7,2),
    ss_ext_list_price         decimal(7,2),
    ss_ext_tax                decimal(7,2),
    ss_coupon_amt             decimal(7,2),
    ss_net_paid               decimal(7,2),
    ss_net_paid_inc_tax       decimal(7,2),
    ss_net_profit             decimal(7,2)
)
FROM FILE '../dbtoaster-experiments-data/tpcds/standard/store_sales.dat'
LINE DELIMITED CSV (delimiter := '|');


CREATE STREAM item
(
    i_item_sk                 integer,
    i_item_id                 char(16),
    i_rec_start_date          date,
    i_rec_end_date            date,
    i_item_desc               varchar(200),
    i_current_price           decimal(7,2),
    i_wholesale_cost          decimal(7,2),
    i_brand_id                integer,
    i_brand                   char(50),
    i_class_id                integer,
    i_class                   char(50),
    i_category_id             integer,
    i_category                char(50),
    i_manufact_id             integer,
    i_manufact                char(50),
    i_size                    char(20),
    i_formulation             char(20),
    i_color                   char(20),
    i_units                   char(10),
    i_container               char(10),
    i_manager_id              integer,
    i_product_name            char(50)
)
FROM FILE '../dbtoaster-experiments-data/tpcds/standard/item.dat'
LINE DELIMITED CSV (delimiter := '|');


CREATE STREAM customer
(
    c_customer_sk             integer,
    c_customer_id             char(16),
    c_current_cdemo_sk        integer,
    c_current_hdemo_sk        integer,
    c_current_addr_sk         integer,
    c_first_shipto_date_sk    integer,
    c_first_sales_date_sk     integer,
    c_salutation              char(10),
    c_first_name              char(20),
    c_last_name               char(30),
    c_preferred_cust_flag     char(1),
    c_birth_day               integer,
    c_birth_month             integer,
    c_birth_year              integer,
    c_birth_country           varchar(20),
    c_login                   char(13),
    c_email_address           char(50),
    c_last_review_date        char(10)
)
FROM FILE '../dbtoaster-experiments-data/tpcds/standard/customer.dat'
LINE DELIMITED CSV (delimiter := '|');


CREATE STREAM customer_address
(
    ca_address_sk             integer,
    ca_address_id             char(16),
    ca_street_number          char(10),
    ca_street_name            varchar(60),
    ca_street_type            char(15),
    ca_suite_number           char(10),
    ca_city                   varchar(60),
    ca_county                 varchar(30),
    ca_state                  char(2),
    ca_zip                    char(10),
    ca_country                varchar(20),
    ca_gmt_offset             decimal(5,2),
    ca_location_type          char(20)
)
FROM FILE '../dbtoaster-experiments-data/tpcds/standard/customer_address.dat'
LINE DELIMITED CSV (delimiter := '|');


CREATE STREAM store
(
    s_store_sk                integer,
    s_store_id                char(16),
    s_rec_start_date          date,
    s_rec_end_date            date,
    s_closed_date_sk          integer,
    s_store_name              varchar(50),
    s_number_employees        integer,
    s_floor_space             integer,
    s_hours                   char(20),
    s_manager                 varchar(40),
    s_market_id               integer,
    s_geography_class         varchar(100),
    s_market_desc             varchar(100),
    s_market_manager          varchar(40),
    s_division_id             integer,
    s_division_name           varchar(50),
    s_company_id              integer,
    s_company_name            varchar(50),
    s_street_number           varchar(10),
    s_street_name             varchar(60),
    s_street_type             char(15),
    s_suite_number            char(10),
    s_city                    varchar(60),
    s_county                  varchar(30),
    s_state                   char(2),
    s_zip                     char(10),
    s_country                 varchar(20),
    s_gmt_offset              decimal(5,2),
    s_tax_precentage          decimal(5,2)
)
FROM FILE '../dbtoaster-experiments-data/tpcds/standard/store.dat'
LINE DELIMITED CSV (delimiter := '|');


CREATE TABLE household_demographics
(
    hd_demo_sk                integer,
    hd_income_band_sk         integer,
    hd_buy_potential          char(15),
    hd_dep_count              integer,
    hd_vehicle_count          integer
)
FROM FILE '../dbtoaster-experiments-data/tpcds/standard/household_demographics.dat'
LINE DELIMITED CSV (delimiter := '|');


CREATE TABLE customer_demographics
(
    cd_demo_sk                integer,
    cd_gender                 char(1),
    cd_marital_status         char(1),
    cd_education_status       char(20),
    cd_purchase_estimate      integer,
    cd_credit_rating          char(10),
    cd_dep_count              integer,
    cd_dep_employed_count     integer,
    cd_dep_college_count      integer
)
FROM FILE '../dbtoaster-experiments-data/tpcds/standard/customer_demographics.dat'
LINE DELIMITED CSV (delimiter := '|');


CREATE TABLE promotion
(
    p_promo_sk                integer,
    p_promo_id                char(16),
    p_start_date_sk           integer,
    p_end_date_sk             integer,
    p_item_sk                 integer,
    p_cost                    decimal(15,2),
    p_response_target         integer,
    p_promo_name              char(50),
    p_channel_dmail           char(1),
    p_channel_email           char(1),
    p_channel_catalog         char(1),
    p_channel_tv              char(1),
    p_channel_radio           char(1),
    p_channel_press           char(1),
    p_channel_event           char(1),
    p_channel_demo            char(1),
    p_channel_details         varchar(100),
    p_purpose                 char(15),
    p_discount_active         char(1)
)
FROM FILE '../dbtoaster-experiments-data/tpcds/standard/promotion.dat'
LINE DELIMITED CSV (delimiter := '|');

