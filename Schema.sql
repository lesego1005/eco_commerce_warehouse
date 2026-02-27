--
-- PostgreSQL database dump
--

\restrict au8TLOONqN99uBtpycxbIeZ9e8vY1IHnOxzx61HrLZqZ3K5Thncv12POgj33M8a

-- Dumped from database version 18.1
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: dim_customer; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_customer (
    customer_id integer NOT NULL,
    customer_name character varying(100) NOT NULL,
    email character varying(100) NOT NULL,
    loyalty_level character varying(20) DEFAULT 'Bronze'::character varying,
    join_date date NOT NULL,
    effective_start timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    effective_end timestamp without time zone DEFAULT 'infinity'::timestamp without time zone NOT NULL,
    is_current boolean DEFAULT true NOT NULL
);


ALTER TABLE public.dim_customer OWNER TO postgres;

--
-- Name: dim_customer_customer_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_customer_customer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_customer_customer_id_seq OWNER TO postgres;

--
-- Name: dim_customer_customer_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.dim_customer_customer_id_seq OWNED BY public.dim_customer.customer_id;


--
-- Name: dim_date; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_date (
    date_id integer NOT NULL,
    date date NOT NULL,
    year integer NOT NULL,
    quarter integer NOT NULL,
    month integer NOT NULL,
    day integer NOT NULL,
    weekday character varying(10) NOT NULL,
    holiday_flag boolean DEFAULT false,
    holiday_name character varying(50),
    CONSTRAINT dim_date_day_check CHECK (((day >= 1) AND (day <= 31))),
    CONSTRAINT dim_date_month_check CHECK (((month >= 1) AND (month <= 12))),
    CONSTRAINT dim_date_quarter_check CHECK (((quarter >= 1) AND (quarter <= 4)))
);


ALTER TABLE public.dim_date OWNER TO postgres;

--
-- Name: dim_date_date_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_date_date_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_date_date_id_seq OWNER TO postgres;

--
-- Name: dim_date_date_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.dim_date_date_id_seq OWNED BY public.dim_date.date_id;


--
-- Name: dim_location; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_location (
    location_id integer NOT NULL,
    city character varying(50) NOT NULL,
    country character varying(50) DEFAULT 'South Africa'::character varying NOT NULL,
    region character varying(50) NOT NULL,
    latitude numeric(9,6),
    longitude numeric(9,6)
);


ALTER TABLE public.dim_location OWNER TO postgres;

--
-- Name: dim_location_location_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_location_location_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_location_location_id_seq OWNER TO postgres;

--
-- Name: dim_location_location_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.dim_location_location_id_seq OWNED BY public.dim_location.location_id;


--
-- Name: dim_product; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_product (
    product_id integer NOT NULL,
    product_name character varying(100) NOT NULL,
    category character varying(50),
    price numeric(10,2),
    carbon_footprint_rating integer,
    effective_start timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    effective_end timestamp without time zone DEFAULT 'infinity'::timestamp without time zone NOT NULL,
    is_current boolean DEFAULT true NOT NULL,
    CONSTRAINT dim_product_carbon_footprint_rating_check CHECK (((carbon_footprint_rating >= 1) AND (carbon_footprint_rating <= 10))),
    CONSTRAINT dim_product_price_check CHECK ((price > (0)::numeric))
);


ALTER TABLE public.dim_product OWNER TO postgres;

--
-- Name: dim_product_product_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_product_product_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_product_product_id_seq OWNER TO postgres;

--
-- Name: dim_product_product_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.dim_product_product_id_seq OWNED BY public.dim_product.product_id;


--
-- Name: fact_sales; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_sales (
    sale_id integer NOT NULL,
    date_id integer NOT NULL,
    product_id integer NOT NULL,
    customer_id integer NOT NULL,
    location_id integer NOT NULL,
    quantity_sold integer NOT NULL,
    revenue numeric(10,2) NOT NULL,
    carbon_savings numeric(10,2) NOT NULL,
    sale_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT fact_sales_carbon_savings_check CHECK ((carbon_savings >= (0)::numeric)),
    CONSTRAINT fact_sales_quantity_sold_check CHECK ((quantity_sold > 0)),
    CONSTRAINT fact_sales_revenue_check CHECK ((revenue >= (0)::numeric))
);


ALTER TABLE public.fact_sales OWNER TO postgres;

--
-- Name: fact_sales_sale_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fact_sales_sale_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fact_sales_sale_id_seq OWNER TO postgres;

--
-- Name: fact_sales_sale_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.fact_sales_sale_id_seq OWNED BY public.fact_sales.sale_id;


--
-- Name: metadata_loads; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.metadata_loads (
    load_id integer NOT NULL,
    load_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    rows_loaded integer NOT NULL,
    status character varying(20) DEFAULT 'SUCCESS'::character varying NOT NULL,
    error_message text
);


ALTER TABLE public.metadata_loads OWNER TO postgres;

--
-- Name: metadata_loads_load_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.metadata_loads_load_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.metadata_loads_load_id_seq OWNER TO postgres;

--
-- Name: metadata_loads_load_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.metadata_loads_load_id_seq OWNED BY public.metadata_loads.load_id;


--
-- Name: dim_customer customer_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_customer ALTER COLUMN customer_id SET DEFAULT nextval('public.dim_customer_customer_id_seq'::regclass);


--
-- Name: dim_date date_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_date ALTER COLUMN date_id SET DEFAULT nextval('public.dim_date_date_id_seq'::regclass);


--
-- Name: dim_location location_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_location ALTER COLUMN location_id SET DEFAULT nextval('public.dim_location_location_id_seq'::regclass);


--
-- Name: dim_product product_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_product ALTER COLUMN product_id SET DEFAULT nextval('public.dim_product_product_id_seq'::regclass);


--
-- Name: fact_sales sale_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_sales ALTER COLUMN sale_id SET DEFAULT nextval('public.fact_sales_sale_id_seq'::regclass);


--
-- Name: metadata_loads load_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metadata_loads ALTER COLUMN load_id SET DEFAULT nextval('public.metadata_loads_load_id_seq'::regclass);


--
-- Name: dim_customer dim_customer_email_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_customer
    ADD CONSTRAINT dim_customer_email_key UNIQUE (email);


--
-- Name: dim_customer dim_customer_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_customer
    ADD CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_id);


--
-- Name: dim_date dim_date_date_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_date
    ADD CONSTRAINT dim_date_date_key UNIQUE (date);


--
-- Name: dim_date dim_date_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_date
    ADD CONSTRAINT dim_date_pkey PRIMARY KEY (date_id);


--
-- Name: dim_location dim_location_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_location
    ADD CONSTRAINT dim_location_pkey PRIMARY KEY (location_id);


--
-- Name: dim_product dim_product_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_product
    ADD CONSTRAINT dim_product_pkey PRIMARY KEY (product_id);


--
-- Name: fact_sales fact_sales_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_sales
    ADD CONSTRAINT fact_sales_pkey PRIMARY KEY (sale_id);


--
-- Name: metadata_loads metadata_loads_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metadata_loads
    ADD CONSTRAINT metadata_loads_pkey PRIMARY KEY (load_id);


--
-- Name: dim_customer unique_email; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_customer
    ADD CONSTRAINT unique_email UNIQUE (email);


--
-- Name: dim_location unique_location; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_location
    ADD CONSTRAINT unique_location UNIQUE (city, region);


--
-- Name: dim_product unique_product_name; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_product
    ADD CONSTRAINT unique_product_name UNIQUE (product_name);


--
-- Name: idx_dim_customer_effective; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dim_customer_effective ON public.dim_customer USING btree (effective_start, effective_end);


--
-- Name: idx_dim_customer_email; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dim_customer_email ON public.dim_customer USING btree (email);


--
-- Name: idx_dim_date_year; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dim_date_year ON public.dim_date USING btree (year);


--
-- Name: idx_dim_product_current; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dim_product_current ON public.dim_product USING btree (product_name) WHERE (is_current = true);


--
-- Name: idx_dim_product_effective; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dim_product_effective ON public.dim_product USING btree (effective_start, effective_end);


--
-- Name: idx_dim_product_name; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dim_product_name ON public.dim_product USING btree (product_name);


--
-- Name: idx_fact_sales_customer_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_sales_customer_id ON public.fact_sales USING btree (customer_id);


--
-- Name: idx_fact_sales_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_sales_date ON public.fact_sales USING btree (date_id);


--
-- Name: idx_fact_sales_date_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_sales_date_id ON public.fact_sales USING btree (date_id);


--
-- Name: idx_fact_sales_date_product; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_sales_date_product ON public.fact_sales USING btree (date_id, product_id);


--
-- Name: idx_fact_sales_location_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_sales_location_id ON public.fact_sales USING btree (location_id);


--
-- Name: idx_fact_sales_product; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_sales_product ON public.fact_sales USING btree (product_id);


--
-- Name: idx_fact_sales_product_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_sales_product_id ON public.fact_sales USING btree (product_id);


--
-- Name: fact_sales fact_sales_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_sales
    ADD CONSTRAINT fact_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.dim_customer(customer_id) ON DELETE RESTRICT;


--
-- Name: fact_sales fact_sales_date_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_sales
    ADD CONSTRAINT fact_sales_date_id_fkey FOREIGN KEY (date_id) REFERENCES public.dim_date(date_id) ON DELETE RESTRICT;


--
-- Name: fact_sales fact_sales_location_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_sales
    ADD CONSTRAINT fact_sales_location_id_fkey FOREIGN KEY (location_id) REFERENCES public.dim_location(location_id) ON DELETE RESTRICT;


--
-- Name: fact_sales fact_sales_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_sales
    ADD CONSTRAINT fact_sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.dim_product(product_id) ON DELETE RESTRICT;


--
-- PostgreSQL database dump complete
--

\unrestrict au8TLOONqN99uBtpycxbIeZ9e8vY1IHnOxzx61HrLZqZ3K5Thncv12POgj33M8a

