--
-- PostgreSQL database dump
--

-- Dumped from database version 15.1
-- Dumped by pg_dump version 15.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: adminpack; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS adminpack WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION adminpack; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION adminpack IS 'administrative functions for PostgreSQL';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: consumer; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.consumer (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.consumer OWNER TO postgres;

--
-- Name: consumer_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.consumer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.consumer_id_seq OWNER TO postgres;

--
-- Name: consumer_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.consumer_id_seq OWNED BY public.consumer.id;


--
-- Name: consumer_topic; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.consumer_topic (
    consumer_id integer NOT NULL,
    topic_id integer NOT NULL,
    pos integer DEFAULT 0 NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.consumer_topic OWNER TO postgres;

--
-- Name: producer; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.producer (
    id integer NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.producer OWNER TO postgres;

--
-- Name: producer_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.producer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.producer_id_seq OWNER TO postgres;

--
-- Name: producer_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.producer_id_seq OWNED BY public.producer.id;


--
-- Name: producer_topic; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.producer_topic (
    producer_id integer NOT NULL,
    topic_id integer NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.producer_topic OWNER TO postgres;

--
-- Name: queue; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.queue (
    index integer NOT NULL,
    topic_id integer NOT NULL,
    message text NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.queue OWNER TO postgres;

--
-- Name: queue_index_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.queue_index_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.queue_index_seq OWNER TO postgres;

--
-- Name: queue_index_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.queue_index_seq OWNED BY public.queue.index;


--
-- Name: topic; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.topic (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.topic OWNER TO postgres;

--
-- Name: topic_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.topic_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.topic_id_seq OWNER TO postgres;

--
-- Name: topic_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.topic_id_seq OWNED BY public.topic.id;


--
-- Name: consumer id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.consumer ALTER COLUMN id SET DEFAULT nextval('public.consumer_id_seq'::regclass);


--
-- Name: producer id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.producer ALTER COLUMN id SET DEFAULT nextval('public.producer_id_seq'::regclass);


--
-- Name: queue index; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.queue ALTER COLUMN index SET DEFAULT nextval('public.queue_index_seq'::regclass);


--
-- Name: topic id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.topic ALTER COLUMN id SET DEFAULT nextval('public.topic_id_seq'::regclass);


--
-- Data for Name: consumer; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.consumer (id, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: consumer_topic; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.consumer_topic (consumer_id, topic_id, pos, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: producer; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.producer (id, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: producer_topic; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.producer_topic (producer_id, topic_id, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: queue; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.queue (index, topic_id, message, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: topic; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.topic (id, name, created_at, updated_at) FROM stdin;
\.


--
-- Name: consumer_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.consumer_id_seq', 1, false);


--
-- Name: producer_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.producer_id_seq', 1, false);


--
-- Name: queue_index_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.queue_index_seq', 1, false);


--
-- Name: topic_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.topic_id_seq', 1, false);


--
-- Name: consumer consumer_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.consumer
    ADD CONSTRAINT consumer_pkey PRIMARY KEY (id);


--
-- Name: consumer_topic consumer_topic_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.consumer_topic
    ADD CONSTRAINT consumer_topic_pkey PRIMARY KEY (consumer_id, topic_id);


--
-- Name: producer producer_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.producer
    ADD CONSTRAINT producer_pkey PRIMARY KEY (id);


--
-- Name: producer_topic producer_topic_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.producer_topic
    ADD CONSTRAINT producer_topic_pkey PRIMARY KEY (producer_id, topic_id);


--
-- Name: queue queue_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.queue
    ADD CONSTRAINT queue_pkey PRIMARY KEY (topic_id, index);


--
-- Name: topic topic_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.topic
    ADD CONSTRAINT topic_pkey PRIMARY KEY (id);


--
-- Name: consumer_topic consumer_topic_consumer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.consumer_topic
    ADD CONSTRAINT consumer_topic_consumer_id_fkey FOREIGN KEY (consumer_id) REFERENCES public.consumer(id);


--
-- Name: consumer_topic consumer_topic_topic_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.consumer_topic
    ADD CONSTRAINT consumer_topic_topic_id_fkey FOREIGN KEY (topic_id) REFERENCES public.topic(id);


--
-- Name: producer_topic producer_topic_producer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.producer_topic
    ADD CONSTRAINT producer_topic_producer_id_fkey FOREIGN KEY (producer_id) REFERENCES public.producer(id);


--
-- Name: producer_topic producer_topic_topic_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.producer_topic
    ADD CONSTRAINT producer_topic_topic_id_fkey FOREIGN KEY (topic_id) REFERENCES public.topic(id);


--
-- Name: queue queue_topic_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.queue
    ADD CONSTRAINT queue_topic_id_fkey FOREIGN KEY (topic_id) REFERENCES public.topic(id);


--
-- PostgreSQL database dump complete
--

