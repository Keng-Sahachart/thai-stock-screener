--
-- PostgreSQL database dump
--

\restrict MtS7JTljYPw77Z5uHcpSeQSYtMzCEObAJV9WifiveLHNi4roHzrm2MNCO5g7neO

-- Dumped from database version 15.15
-- Dumped by pg_dump version 18.4

-- Started on 2026-09-04 13:32:56

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

--
-- TOC entry 267 (class 1255 OID 16390)
-- Name: refresh_indicator_view(); Type: FUNCTION; Schema: public; Owner: AdminKeng
--

CREATE FUNCTION public.refresh_indicator_view() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    keys text[];
    current_cols text[];
    diff_keys text[];
    key  text;
    sql  text;
    view_exists boolean;
BEGIN
    -- ดึง Keys ทั้งหมดที่มีอยู่ในตาราง JSONB
    SELECT array_agg(DISTINCT j.key) INTO keys
    FROM stock_indicator_jsonb s
    CROSS JOIN LATERAL jsonb_object_keys(s.indicators) AS j(key);

    IF keys IS NULL OR array_length(keys, 1) IS NULL THEN
        RAISE NOTICE 'No keys found in stock_indicator_jsonb.';
        RETURN;
    END IF;

    -- ตรวจสอบว่ามี Materialized View อยู่แล้วหรือไม่
    SELECT EXISTS (
        SELECT 1 
        FROM pg_matviews 
        WHERE schemaname = 'public' AND matviewname = 'mv_stock_indicators'
    ) INTO view_exists;

    -- กรณีที่ 1: มี View อยู่แล้ว
    IF view_exists THEN
        -- ดึงรายชื่อ Column ปัจจุบันของ View (ตัด symbol และ trade_date ออก)
        SELECT array_agg(attname::text) INTO current_cols
        FROM pg_attribute
        WHERE attrelid = 'public.mv_stock_indicators'::regclass
          AND attnum > 0 
          AND NOT attisdropped
          AND attname NOT IN ('symbol', 'trade_date');

        -- เช็คว่ามี Keys ใหม่งอกมาเกินกว่า Columns เดิมหรือไม่
        SELECT array_agg(k) INTO diff_keys
        FROM unnest(keys) k
        WHERE k NOT IN (SELECT unnest(current_cols));

        -- ถ้า Keys เท่าเดิมเป๊ะ -> สั่ง Refresh ข้อมูลอย่างเดียว ปลอดภัย 100% ต่อ View อื่น
        IF diff_keys IS NULL OR array_length(diff_keys, 1) IS NULL THEN
            REFRESH MATERIALIZED VIEW public.mv_stock_indicators;
            RAISE NOTICE 'Materialized View refreshed successfully (No schema changes).';
            RETURN;
        ELSE
            -- ถ้ามี Keys ใหม่ จะไม่ Drop ทิ้งอัตโนมัติ เพื่อป้องกัน View อื่นพัง
            RAISE EXCEPTION 'Cannot auto-recreate: Found new indicator keys % that are not in current View. Recreating will break dependent views.', diff_keys;
        END IF;
    END IF;

    -- กรณีที่ 2: เพิ่งรันครั้งแรก (ยังไม่มี View ในระบบ)
    sql := 'CREATE MATERIALIZED VIEW public.mv_stock_indicators AS SELECT symbol, trade_date';

    FOREACH key IN ARRAY keys LOOP
        IF key ~ '^trend_status' THEN 
            sql := sql || format(', nullif(indicators ->> %L,''null'')::text AS %I', key, key);
        ELSE
            sql := sql || format(', nullif(indicators ->> %L,''null'')::numeric AS %I', key, key);
        END IF;
    END LOOP;

    sql := sql || ' FROM stock_indicator_jsonb;';
    EXECUTE sql;

    -- สร้าง Unique Index รองรับการขยายผล
    EXECUTE 'CREATE UNIQUE INDEX idx_mv_stock_ind_sym_date ON public.mv_stock_indicators (symbol, trade_date);';
    EXECUTE 'CREATE INDEX idx_mv_stock_ind_date ON public.mv_stock_indicators (trade_date);';

    RAISE NOTICE 'Materialized View mv_stock_indicators created initially with % keys and indexes.', array_length(keys, 1);
END;
$$;


ALTER FUNCTION public.refresh_indicator_view() OWNER TO "AdminKeng";

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 250 (class 1259 OID 183689)
-- Name: account_info_history; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.account_info_history (
    id integer NOT NULL,
    account_no character varying(50) NOT NULL,
    is_disabled boolean DEFAULT false NOT NULL,
    import_date timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    line_available numeric(18,4),
    credit_limit numeric(18,4),
    cash_balance numeric(18,4),
    account_type text,
    client_type text,
    customer_type text,
    can_buy boolean,
    can_sell boolean,
    crossing_key text,
    initial_credit_limit numeric(18,4),
    initial_cash_balance numeric(18,4),
    initial_line_available numeric(18,4),
    net_settlement_line numeric(18,4),
    cash_type text,
    collateral numeric(18,4),
    credit_balance boolean
);


ALTER TABLE public.account_info_history OWNER TO "AdminKeng";

--
-- TOC entry 249 (class 1259 OID 183688)
-- Name: account_info_history_id_seq; Type: SEQUENCE; Schema: public; Owner: AdminKeng
--

CREATE SEQUENCE public.account_info_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.account_info_history_id_seq OWNER TO "AdminKeng";

--
-- TOC entry 3613 (class 0 OID 0)
-- Dependencies: 249
-- Name: account_info_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: AdminKeng
--

ALTER SEQUENCE public.account_info_history_id_seq OWNED BY public.account_info_history.id;


--
-- TOC entry 229 (class 1259 OID 16430)
-- Name: stock_price_history; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stock_price_history (
    symbol character varying(100) NOT NULL,
    date date NOT NULL,
    open numeric(18,6),
    high numeric(18,6),
    low numeric(18,6),
    close numeric(18,6),
    volume bigint,
    import_datetime timestamp without time zone DEFAULT now()
);


ALTER TABLE public.stock_price_history OWNER TO "AdminKeng";

--
-- TOC entry 3614 (class 0 OID 0)
-- Dependencies: 229
-- Name: TABLE stock_price_history; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.stock_price_history IS 'เก็บ ราคาหุ้นต่างๆ end of day จาก api ของ settrade';


--
-- TOC entry 233 (class 1259 OID 16443)
-- Name: stock_signal; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stock_signal (
    symbol text NOT NULL,
    trade_date date NOT NULL,
    signal_type text,
    priority integer,
    reason text,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.stock_signal OWNER TO "AdminKeng";

--
-- TOC entry 3615 (class 0 OID 0)
-- Dependencies: 233
-- Name: TABLE stock_signal; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.stock_signal IS 'ตารางสัญญาณที่คำนวณได้ ';


--
-- TOC entry 241 (class 1259 OID 16555)
-- Name: backtest; Type: VIEW; Schema: public; Owner: AdminKeng
--

CREATE VIEW public.backtest AS
 WITH future_prices AS (
         SELECT stock_price_history.symbol,
            stock_price_history.date AS trade_date,
            stock_price_history.close AS entry_price,
            lead(stock_price_history.close, 5) OVER (PARTITION BY stock_price_history.symbol ORDER BY stock_price_history.date) AS price_day_5,
            lead(stock_price_history.close, 10) OVER (PARTITION BY stock_price_history.symbol ORDER BY stock_price_history.date) AS price_day_10,
            max(stock_price_history.high) OVER (PARTITION BY stock_price_history.symbol ORDER BY stock_price_history.date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) AS max_high_10d
           FROM public.stock_price_history
        ), signal_performance AS (
         SELECT s.symbol,
            s.trade_date,
            s.signal_type,
            f.entry_price,
            f.price_day_5,
            f.price_day_10,
            f.max_high_10d
           FROM (public.stock_signal s
             JOIN future_prices f ON (((s.symbol = (f.symbol)::text) AND (s.trade_date = f.trade_date))))
          WHERE (s.signal_type = ANY (ARRAY['BUY'::text, 'BUY-STRONG'::text]))
        )
 SELECT signal_performance.symbol,
    signal_performance.trade_date,
    signal_performance.signal_type,
    signal_performance.entry_price,
    round((((signal_performance.price_day_5 - signal_performance.entry_price) / signal_performance.entry_price) * (100)::numeric), 2) AS return_5d_pct,
    round((((signal_performance.price_day_10 - signal_performance.entry_price) / signal_performance.entry_price) * (100)::numeric), 2) AS return_10d_pct,
    round((((signal_performance.max_high_10d - signal_performance.entry_price) / signal_performance.entry_price) * (100)::numeric), 2) AS max_upside_pct
   FROM signal_performance
  WHERE (signal_performance.price_day_10 IS NOT NULL)
  ORDER BY signal_performance.trade_date DESC, signal_performance.symbol;


ALTER VIEW public.backtest OWNER TO "AdminKeng";

--
-- TOC entry 244 (class 1259 OID 179686)
-- Name: bot_active_positions; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.bot_active_positions (
    id integer NOT NULL,
    symbol character varying(50) NOT NULL,
    entry_date date NOT NULL,
    entry_price numeric(18,4) NOT NULL,
    entry_atr14 numeric(18,4),
    initial_stop_loss numeric(18,4) NOT NULL,
    max_price_reached numeric(18,4) NOT NULL,
    trailing_stop_loss numeric(18,4),
    current_volume integer DEFAULT 0 NOT NULL,
    is_managed_by_bot boolean DEFAULT true NOT NULL,
    status character varying(20) DEFAULT 'OPEN'::character varying NOT NULL,
    closed_date date,
    closed_price numeric(18,4),
    exit_reason text,
    created_at timestamp without time zone DEFAULT timezone('Asia/Bangkok'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('Asia/Bangkok'::text, now())
);


ALTER TABLE public.bot_active_positions OWNER TO "AdminKeng";

--
-- TOC entry 3616 (class 0 OID 0)
-- Dependencies: 244
-- Name: TABLE bot_active_positions; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.bot_active_positions IS 'ตารางบันทึกสถานะไม้ที่บอทดูแล (Snapshot ไม้แรก & Trailing Stop)';


--
-- TOC entry 3617 (class 0 OID 0)
-- Dependencies: 244
-- Name: COLUMN bot_active_positions.status; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.bot_active_positions.status IS '''OPEN'', ''CLOSED''';


--
-- TOC entry 243 (class 1259 OID 179685)
-- Name: bot_active_positions_id_seq; Type: SEQUENCE; Schema: public; Owner: AdminKeng
--

CREATE SEQUENCE public.bot_active_positions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.bot_active_positions_id_seq OWNER TO "AdminKeng";

--
-- TOC entry 3618 (class 0 OID 0)
-- Dependencies: 243
-- Name: bot_active_positions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: AdminKeng
--

ALTER SEQUENCE public.bot_active_positions_id_seq OWNED BY public.bot_active_positions.id;


--
-- TOC entry 248 (class 1259 OID 179713)
-- Name: bot_orders; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.bot_orders (
    order_id integer NOT NULL,
    signal_id integer,
    symbol character varying(50) NOT NULL,
    side character varying(10) NOT NULL,
    order_type character varying(20) DEFAULT 'LIMIT'::character varying NOT NULL,
    volume integer NOT NULL,
    target_price numeric(18,4) NOT NULL,
    executed_price numeric(18,4),
    status character varying(20) DEFAULT 'DRY_RUN'::character varying NOT NULL,
    broker_order_no character varying(100),
    error_message text,
    created_at timestamp without time zone DEFAULT timezone('Asia/Bangkok'::text, now()),
    executed_at timestamp without time zone
);


ALTER TABLE public.bot_orders OWNER TO "AdminKeng";

--
-- TOC entry 3619 (class 0 OID 0)
-- Dependencies: 248
-- Name: TABLE bot_orders; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.bot_orders IS 'ตารางควบคุมสถานะคำสั่งซื้อขาย (Order Lifecycle)';


--
-- TOC entry 3620 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN bot_orders.side; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.bot_orders.side IS '''BUY'', ''SELL''';


--
-- TOC entry 3621 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN bot_orders.status; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.bot_orders.status IS '''DRY_RUN'', ''PENDING'', ''SENT'', ''FILLED'', ''REJECTED'', ''CANCELLED''';


--
-- TOC entry 247 (class 1259 OID 179712)
-- Name: bot_orders_order_id_seq; Type: SEQUENCE; Schema: public; Owner: AdminKeng
--

CREATE SEQUENCE public.bot_orders_order_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.bot_orders_order_id_seq OWNER TO "AdminKeng";

--
-- TOC entry 3622 (class 0 OID 0)
-- Dependencies: 247
-- Name: bot_orders_order_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: AdminKeng
--

ALTER SEQUENCE public.bot_orders_order_id_seq OWNED BY public.bot_orders.order_id;


--
-- TOC entry 246 (class 1259 OID 179701)
-- Name: bot_trade_signals; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.bot_trade_signals (
    id integer NOT NULL,
    symbol character varying(50) NOT NULL,
    trade_date date NOT NULL,
    signal_type character varying(50) NOT NULL,
    signal_source character varying(100) NOT NULL,
    trigger_price numeric(18,4),
    stop_loss_plan numeric(18,4),
    recommended_shares integer,
    reason text,
    status character varying(20) DEFAULT 'PENDING'::character varying NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('Asia/Bangkok'::text, now()),
    expired_at timestamp without time zone,
    priority integer DEFAULT 3
);


ALTER TABLE public.bot_trade_signals OWNER TO "AdminKeng";

--
-- TOC entry 3623 (class 0 OID 0)
-- Dependencies: 246
-- Name: TABLE bot_trade_signals; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.bot_trade_signals IS 'ตารางบันทึกสัญญาณที่สแกนพบ (รอการยืนยันทาง Telegram)';


--
-- TOC entry 3624 (class 0 OID 0)
-- Dependencies: 246
-- Name: COLUMN bot_trade_signals.signal_type; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.bot_trade_signals.signal_type IS '''BUY'', ''BUY-STRONG'', ''SELL''';


--
-- TOC entry 3625 (class 0 OID 0)
-- Dependencies: 246
-- Name: COLUMN bot_trade_signals.signal_source; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.bot_trade_signals.signal_source IS 'แหล่งที่มาของสัญญาณ เช่น ''v_macd_advanced_analysis''';


--
-- TOC entry 3626 (class 0 OID 0)
-- Dependencies: 246
-- Name: COLUMN bot_trade_signals.status; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.bot_trade_signals.status IS '''PENDING'', ''APPROVED'', ''REJECTED'', ''EXPIRED''';


--
-- TOC entry 245 (class 1259 OID 179700)
-- Name: bot_trade_signals_id_seq; Type: SEQUENCE; Schema: public; Owner: AdminKeng
--

CREATE SEQUENCE public.bot_trade_signals_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.bot_trade_signals_id_seq OWNER TO "AdminKeng";

--
-- TOC entry 3627 (class 0 OID 0)
-- Dependencies: 245
-- Name: bot_trade_signals_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: AdminKeng
--

ALTER SEQUENCE public.bot_trade_signals_id_seq OWNED BY public.bot_trade_signals.id;


--
-- TOC entry 222 (class 1259 OID 16391)
-- Name: dim_symbol_th; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.dim_symbol_th (
    symbol text NOT NULL,
    exchange text NOT NULL,
    full_code text,
    name_en text,
    currency text,
    asset_type text,
    market text,
    board text,
    sector text,
    industry text,
    lot_size integer,
    tick_size numeric(18,6),
    is_tradable boolean,
    provider_raw jsonb,
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.dim_symbol_th OWNER TO "AdminKeng";

--
-- TOC entry 242 (class 1259 OID 175849)
-- Name: master_stock_classification; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.master_stock_classification (
    symbol character varying(50) NOT NULL,
    short_name character varying(255),
    quote_type character varying(50),
    sector character varying(100),
    industry character varying(150),
    market_cap numeric,
    currency character varying(10),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.master_stock_classification OWNER TO "AdminKeng";

--
-- TOC entry 227 (class 1259 OID 16419)
-- Name: stock_indicator_jsonb; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stock_indicator_jsonb (
    symbol text NOT NULL,
    trade_date date NOT NULL,
    indicators jsonb NOT NULL,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.stock_indicator_jsonb OWNER TO "AdminKeng";

--
-- TOC entry 3628 (class 0 OID 0)
-- Dependencies: 227
-- Name: TABLE stock_indicator_jsonb; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.stock_indicator_jsonb IS 'บันทึก indicator ที่มาจากการคำนวน เก็บไว้ในรูปแบบ json เพื่อความยืดหยุ่น';


--
-- TOC entry 251 (class 1259 OID 187589)
-- Name: mv_stock_indicators; Type: MATERIALIZED VIEW; Schema: public; Owner: AdminKeng
--

CREATE MATERIALIZED VIEW public.mv_stock_indicators AS
 SELECT stock_indicator_jsonb.symbol,
    stock_indicator_jsonb.trade_date,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'atr14'::text), 'null'::text))::numeric AS atr14,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'bb_lower'::text), 'null'::text))::numeric AS bb_lower,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'bb_mid'::text), 'null'::text))::numeric AS bb_mid,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'bb_upper'::text), 'null'::text))::numeric AS bb_upper,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema10'::text), 'null'::text))::numeric AS ema10,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema12'::text), 'null'::text))::numeric AS ema12,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema20'::text), 'null'::text))::numeric AS ema20,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema200'::text), 'null'::text))::numeric AS ema200,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema26'::text), 'null'::text))::numeric AS ema26,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema5'::text), 'null'::text))::numeric AS ema5,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema50'::text), 'null'::text))::numeric AS ema50,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_12_26_9'::text), 'null'::text))::numeric AS macd_12_26_9,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_12_26_9_hist'::text), 'null'::text))::numeric AS macd_12_26_9_hist,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_12_26_9_signal'::text), 'null'::text))::numeric AS macd_12_26_9_signal,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_19_39_9'::text), 'null'::text))::numeric AS macd_19_39_9,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_19_39_9_hist'::text), 'null'::text))::numeric AS macd_19_39_9_hist,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_19_39_9_signal'::text), 'null'::text))::numeric AS macd_19_39_9_signal,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'rsi14'::text), 'null'::text))::numeric AS rsi14,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'rsi21'::text), 'null'::text))::numeric AS rsi21,
    NULLIF((stock_indicator_jsonb.indicators ->> 'trend_status'::text), 'null'::text) AS trend_status,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'volume_avg20'::text), 'null'::text))::numeric AS volume_avg20,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'volume_ema20'::text), 'null'::text))::numeric AS volume_ema20,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'volume_ema5'::text), 'null'::text))::numeric AS volume_ema5,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'volume_ema50'::text), 'null'::text))::numeric AS volume_ema50
   FROM public.stock_indicator_jsonb
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.mv_stock_indicators OWNER TO "AdminKeng";

--
-- TOC entry 223 (class 1259 OID 16397)
-- Name: portfolio_stock; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.portfolio_stock (
    account_no character varying(255),
    imported_at character varying(255),
    symbol character varying(255),
    flag character varying(255),
    nvdr_flag character varying(255),
    market_price real,
    amount real,
    market_description character varying(255),
    market_value real,
    profit real,
    percent_profit real,
    realize_profit real,
    start_volume integer,
    current_volume integer,
    actual_volume integer,
    start_price real,
    average_price real,
    show_na boolean,
    port_flag character varying(255),
    margin_rate real,
    liabilities integer,
    commission_rate real,
    vat_rate real
);


ALTER TABLE public.portfolio_stock OWNER TO "AdminKeng";

--
-- TOC entry 3629 (class 0 OID 0)
-- Dependencies: 223
-- Name: TABLE portfolio_stock; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.portfolio_stock IS 'รายละเอียด รายชื่อหุ้นใน Port';


--
-- TOC entry 224 (class 1259 OID 16402)
-- Name: settrade_stocklist; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.settrade_stocklist (
    symbol character varying(255),
    name_th character varying(255),
    name_en character varying(255),
    market character varying(255)
);


ALTER TABLE public.settrade_stocklist OWNER TO "AdminKeng";

--
-- TOC entry 3630 (class 0 OID 0)
-- Dependencies: 224
-- Name: TABLE settrade_stocklist; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.settrade_stocklist IS 'ข้อมูล รายชื่อ หุ้น ที่ดึงจาก เว็บ settrade';


--
-- TOC entry 225 (class 1259 OID 16407)
-- Name: stock_indicator_daily; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stock_indicator_daily (
    symbol text NOT NULL,
    trade_date date NOT NULL,
    ema20 numeric(18,6),
    ema50 numeric(18,6),
    ema200 numeric(18,6),
    rsi14 numeric(18,6),
    macd numeric(18,6),
    macd_signal numeric(18,6),
    macd_hist numeric(18,6),
    volume_avg20 numeric(18,2),
    trend_status text,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.stock_indicator_daily OWNER TO "AdminKeng";

--
-- TOC entry 3631 (class 0 OID 0)
-- Dependencies: 225
-- Name: TABLE stock_indicator_daily; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.stock_indicator_daily IS 'ข้อมูล indicator รายวันที่คำนวณจาก compute_indicators';


--
-- TOC entry 226 (class 1259 OID 16413)
-- Name: stock_indicator_daily_v4; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stock_indicator_daily_v4 (
    symbol text NOT NULL,
    trade_date date NOT NULL,
    ema5 numeric(18,6),
    ema10 numeric(18,6),
    ema12 numeric(18,6),
    ema20 numeric(18,6),
    ema26 numeric(18,6),
    ema50 numeric(18,6),
    ema200 numeric(18,6),
    rsi14 numeric(18,6),
    rsi21 numeric(18,6),
    macd numeric(18,6),
    macd_signal numeric(18,6),
    macd_hist numeric(18,6),
    macd_19_39_9 numeric(18,6),
    macd_19_39_9_signal numeric(18,6),
    macd_19_39_9_hist numeric(18,6),
    volume_avg20 numeric(18,2),
    trend_status text,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.stock_indicator_daily_v4 OWNER TO "AdminKeng";

--
-- TOC entry 228 (class 1259 OID 16425)
-- Name: stock_list_info_siamchart; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stock_list_info_siamchart (
    name character varying(255),
    no real,
    links character varying(255),
    sign character varying(255),
    last real,
    chg real,
    volume real,
    valuek real,
    mcapm real,
    pe real,
    pbv real,
    de real,
    dps real,
    eps real,
    roa real,
    roe real,
    npm real,
    yield real,
    ffloat real,
    mg real,
    magic1 real,
    magic2 real,
    peg real,
    cg real,
    import_datetime text
);


ALTER TABLE public.stock_list_info_siamchart OWNER TO "AdminKeng";

--
-- TOC entry 3632 (class 0 OID 0)
-- Dependencies: 228
-- Name: TABLE stock_list_info_siamchart; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.stock_list_info_siamchart IS 'รายชื่อหุ้น และ ค่าของหุ้นต่างๆ ที่ดึง scarp จากเว็บ siamchart';


--
-- TOC entry 230 (class 1259 OID 16433)
-- Name: stock_price_history_archive1975_2018; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stock_price_history_archive1975_2018 (
    symbol character varying(100),
    date date,
    open numeric(18,6),
    high numeric(18,6),
    low numeric(18,6),
    close numeric(18,6),
    volume bigint
);


ALTER TABLE public.stock_price_history_archive1975_2018 OWNER TO "AdminKeng";

--
-- TOC entry 3633 (class 0 OID 0)
-- Dependencies: 230
-- Name: TABLE stock_price_history_archive1975_2018; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON TABLE public.stock_price_history_archive1975_2018 IS 'ราคาหุ้นย้อนหลัง backup ไว้ ปี 1975-2018 ';


--
-- TOC entry 231 (class 1259 OID 16436)
-- Name: stock_prices; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stock_prices (
    id integer NOT NULL,
    symbol text NOT NULL,
    price numeric(20,6) NOT NULL,
    volume bigint,
    price_time timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.stock_prices OWNER TO "AdminKeng";

--
-- TOC entry 232 (class 1259 OID 16442)
-- Name: stock_prices_id_seq; Type: SEQUENCE; Schema: public; Owner: AdminKeng
--

CREATE SEQUENCE public.stock_prices_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.stock_prices_id_seq OWNER TO "AdminKeng";

--
-- TOC entry 3634 (class 0 OID 0)
-- Dependencies: 232
-- Name: stock_prices_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: AdminKeng
--

ALTER SEQUENCE public.stock_prices_id_seq OWNED BY public.stock_prices.id;


--
-- TOC entry 238 (class 1259 OID 16510)
-- Name: stock_value_score; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stock_value_score (
    name text,
    value_score double precision,
    rank integer,
    cg_score double precision,
    de_score double precision,
    dps_score double precision,
    eps_score double precision,
    mg_score double precision,
    npm_score double precision,
    pbv_score double precision,
    pe_score double precision,
    peg_score double precision,
    roa_score double precision,
    roe_score double precision,
    yield_score double precision
);


ALTER TABLE public.stock_value_score OWNER TO "AdminKeng";

--
-- TOC entry 234 (class 1259 OID 16454)
-- Name: stocklist_twelvedata; Type: TABLE; Schema: public; Owner: AdminKeng
--

CREATE TABLE public.stocklist_twelvedata (
    id integer NOT NULL,
    symbol character varying(20),
    name text,
    exchange character varying(10),
    currency character varying(10),
    mic_code character varying(10),
    country character varying(50),
    type_ character varying(50)
);


ALTER TABLE public.stocklist_twelvedata OWNER TO "AdminKeng";

--
-- TOC entry 235 (class 1259 OID 16459)
-- Name: stocklist_twelvedata_id_seq; Type: SEQUENCE; Schema: public; Owner: AdminKeng
--

CREATE SEQUENCE public.stocklist_twelvedata_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.stocklist_twelvedata_id_seq OWNER TO "AdminKeng";

--
-- TOC entry 3635 (class 0 OID 0)
-- Dependencies: 235
-- Name: stocklist_twelvedata_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: AdminKeng
--

ALTER SEQUENCE public.stocklist_twelvedata_id_seq OWNED BY public.stocklist_twelvedata.id;


--
-- TOC entry 236 (class 1259 OID 16460)
-- Name: tablesize; Type: VIEW; Schema: public; Owner: AdminKeng
--

CREATE VIEW public.tablesize AS
 SELECT pg_statio_user_tables.schemaname,
    pg_statio_user_tables.relname AS table_name,
    pg_size_pretty(pg_total_relation_size((pg_statio_user_tables.relid)::regclass)) AS total_size,
    pg_size_pretty(pg_relation_size((pg_statio_user_tables.relid)::regclass)) AS table_size,
    pg_size_pretty((pg_total_relation_size((pg_statio_user_tables.relid)::regclass) - pg_relation_size((pg_statio_user_tables.relid)::regclass))) AS index_size,
    pg_total_relation_size((pg_statio_user_tables.relid)::regclass) AS total_bytes
   FROM pg_statio_user_tables
  ORDER BY (pg_total_relation_size((pg_statio_user_tables.relid)::regclass)) DESC;


ALTER VIEW public.tablesize OWNER TO "AdminKeng";

--
-- TOC entry 253 (class 1259 OID 187878)
-- Name: v_macd_advanced_analysis; Type: VIEW; Schema: public; Owner: AdminKeng
--

CREATE VIEW public.v_macd_advanced_analysis AS
 WITH macd_raw AS (
         SELECT i.symbol,
            i.trade_date,
            p.close,
            p.volume,
            i.volume_ema20,
            i.volume_ema50,
            i.macd_12_26_9,
            i.macd_12_26_9_signal,
            i.macd_12_26_9_hist,
            i.macd_19_39_9,
            i.macd_19_39_9_signal,
            i.macd_19_39_9_hist,
            lag(i.macd_12_26_9) OVER (PARTITION BY i.symbol ORDER BY i.trade_date) AS prev_macd_12,
            lag(i.macd_12_26_9_signal) OVER (PARTITION BY i.symbol ORDER BY i.trade_date) AS prev_sig_12,
            lag(i.macd_19_39_9) OVER (PARTITION BY i.symbol ORDER BY i.trade_date) AS prev_macd_19,
            lag(i.macd_19_39_9_signal) OVER (PARTITION BY i.symbol ORDER BY i.trade_date) AS prev_sig_19,
            lag(i.macd_12_26_9_hist) OVER (PARTITION BY i.symbol ORDER BY i.trade_date) AS prev_hist_12,
            lag(i.macd_19_39_9_hist) OVER (PARTITION BY i.symbol ORDER BY i.trade_date) AS prev_hist_19
           FROM (public.mv_stock_indicators i
             LEFT JOIN public.stock_price_history p ON (((i.symbol = (p.symbol)::text) AND (i.trade_date = p.date))))
        ), cross_events AS (
         SELECT macd_raw.symbol,
            macd_raw.trade_date,
            macd_raw.close,
            macd_raw.volume,
            macd_raw.volume_ema20,
            macd_raw.volume_ema50,
            macd_raw.macd_12_26_9,
            macd_raw.macd_12_26_9_signal,
            macd_raw.macd_12_26_9_hist,
            macd_raw.macd_19_39_9,
            macd_raw.macd_19_39_9_signal,
            macd_raw.macd_19_39_9_hist,
            macd_raw.prev_macd_12,
            macd_raw.prev_sig_12,
            macd_raw.prev_macd_19,
            macd_raw.prev_sig_19,
            macd_raw.prev_hist_12,
            macd_raw.prev_hist_19,
            (macd_raw.macd_12_26_9_hist - macd_raw.prev_hist_12) AS hist_slope_12,
            (macd_raw.macd_19_39_9_hist - macd_raw.prev_hist_19) AS hist_slope_19,
            round((((macd_raw.macd_12_26_9_hist - macd_raw.prev_hist_12) / NULLIF(macd_raw.close, (0)::numeric)) * (100)::numeric), 4) AS hist_accel_pct_12,
            round((((macd_raw.macd_19_39_9_hist - macd_raw.prev_hist_19) / NULLIF(macd_raw.close, (0)::numeric)) * (100)::numeric), 4) AS hist_accel_pct_19,
            round(((macd_raw.volume)::numeric / NULLIF(macd_raw.volume_ema20, (0)::numeric)), 2) AS vol_surge_ratio,
                CASE
                    WHEN ((macd_raw.prev_macd_12 < (0)::numeric) AND (macd_raw.macd_12_26_9 >= (0)::numeric)) THEN 'CROSS_UP_ZERO'::text
                    WHEN ((macd_raw.prev_macd_12 > (0)::numeric) AND (macd_raw.macd_12_26_9 <= (0)::numeric)) THEN 'CROSS_DOWN_ZERO'::text
                    ELSE NULL::text
                END AS cross_zero_12_event,
                CASE
                    WHEN ((macd_raw.prev_macd_12 < macd_raw.prev_sig_12) AND (macd_raw.macd_12_26_9 >= macd_raw.macd_12_26_9_signal)) THEN 'GOLDEN_CROSS'::text
                    WHEN ((macd_raw.prev_macd_12 > macd_raw.prev_sig_12) AND (macd_raw.macd_12_26_9 <= macd_raw.macd_12_26_9_signal)) THEN 'DEAD_CROSS'::text
                    ELSE NULL::text
                END AS cross_sig_12_event,
                CASE
                    WHEN ((macd_raw.macd_12_26_9 > (0)::numeric) AND (macd_raw.macd_19_39_9 > (0)::numeric) AND (macd_raw.macd_12_26_9 > macd_raw.macd_12_26_9_signal)) THEN 'STRONG_BUY_ZONE'::text
                    WHEN (macd_raw.macd_12_26_9 > macd_raw.macd_12_26_9_signal) THEN 'BUY_ZONE'::text
                    WHEN ((macd_raw.macd_12_26_9 < (0)::numeric) AND (macd_raw.macd_19_39_9 < (0)::numeric) AND (macd_raw.macd_12_26_9 < macd_raw.macd_12_26_9_signal)) THEN 'STRONG_SELL_ZONE'::text
                    ELSE 'SELL_ZONE'::text
                END AS market_zone
           FROM macd_raw
        ), zone_lagged AS (
         SELECT cross_events.symbol,
            cross_events.trade_date,
            cross_events.close,
            cross_events.volume,
            cross_events.volume_ema20,
            cross_events.volume_ema50,
            cross_events.macd_12_26_9,
            cross_events.macd_12_26_9_signal,
            cross_events.macd_12_26_9_hist,
            cross_events.macd_19_39_9,
            cross_events.macd_19_39_9_signal,
            cross_events.macd_19_39_9_hist,
            cross_events.prev_macd_12,
            cross_events.prev_sig_12,
            cross_events.prev_macd_19,
            cross_events.prev_sig_19,
            cross_events.prev_hist_12,
            cross_events.prev_hist_19,
            cross_events.hist_slope_12,
            cross_events.hist_slope_19,
            cross_events.hist_accel_pct_12,
            cross_events.hist_accel_pct_19,
            cross_events.vol_surge_ratio,
            cross_events.cross_zero_12_event,
            cross_events.cross_sig_12_event,
            cross_events.market_zone,
            lag(cross_events.market_zone) OVER (PARTITION BY cross_events.symbol ORDER BY cross_events.trade_date) AS prev_market_zone
           FROM cross_events
        ), grouped_streaks AS (
         SELECT zone_lagged.symbol,
            zone_lagged.trade_date,
            zone_lagged.close,
            zone_lagged.volume,
            zone_lagged.volume_ema20,
            zone_lagged.volume_ema50,
            zone_lagged.macd_12_26_9,
            zone_lagged.macd_12_26_9_signal,
            zone_lagged.macd_12_26_9_hist,
            zone_lagged.macd_19_39_9,
            zone_lagged.macd_19_39_9_signal,
            zone_lagged.macd_19_39_9_hist,
            zone_lagged.prev_macd_12,
            zone_lagged.prev_sig_12,
            zone_lagged.prev_macd_19,
            zone_lagged.prev_sig_19,
            zone_lagged.prev_hist_12,
            zone_lagged.prev_hist_19,
            zone_lagged.hist_slope_12,
            zone_lagged.hist_slope_19,
            zone_lagged.hist_accel_pct_12,
            zone_lagged.hist_accel_pct_19,
            zone_lagged.vol_surge_ratio,
            zone_lagged.cross_zero_12_event,
            zone_lagged.cross_sig_12_event,
            zone_lagged.market_zone,
            zone_lagged.prev_market_zone,
            count(zone_lagged.cross_zero_12_event) OVER (PARTITION BY zone_lagged.symbol ORDER BY zone_lagged.trade_date) AS grp_cross_zero,
            count(zone_lagged.cross_sig_12_event) OVER (PARTITION BY zone_lagged.symbol ORDER BY zone_lagged.trade_date) AS grp_cross_sig,
            sum(
                CASE
                    WHEN (zone_lagged.prev_market_zone = zone_lagged.market_zone) THEN 0
                    ELSE 1
                END) OVER (PARTITION BY zone_lagged.symbol ORDER BY zone_lagged.trade_date) AS grp_zone
           FROM zone_lagged
        )
 SELECT grouped_streaks.symbol,
    grouped_streaks.trade_date,
    grouped_streaks.close,
    grouped_streaks.volume,
    grouped_streaks.volume_ema20,
    grouped_streaks.vol_surge_ratio,
    grouped_streaks.macd_12_26_9,
    grouped_streaks.macd_12_26_9_signal,
    grouped_streaks.macd_12_26_9_hist,
    grouped_streaks.hist_slope_12 AS macd_12_26_9_hist_slope,
    grouped_streaks.hist_accel_pct_12 AS macd_12_26_9_hist_accel_pct,
    grouped_streaks.macd_19_39_9,
    grouped_streaks.macd_19_39_9_signal,
    grouped_streaks.macd_19_39_9_hist,
    grouped_streaks.hist_slope_19 AS macd_19_39_9_hist_slope,
    grouped_streaks.hist_accel_pct_19 AS macd_19_39_9_hist_accel_pct,
    last_value(grouped_streaks.cross_zero_12_event) OVER (PARTITION BY grouped_streaks.symbol, grouped_streaks.grp_cross_zero ORDER BY grouped_streaks.trade_date) AS last_zero_cross_type,
    (row_number() OVER (PARTITION BY grouped_streaks.symbol, grouped_streaks.grp_cross_zero ORDER BY grouped_streaks.trade_date) - 1) AS days_since_zero_cross,
    last_value(grouped_streaks.cross_sig_12_event) OVER (PARTITION BY grouped_streaks.symbol, grouped_streaks.grp_cross_sig ORDER BY grouped_streaks.trade_date) AS last_signal_cross_type,
    (row_number() OVER (PARTITION BY grouped_streaks.symbol, grouped_streaks.grp_cross_sig ORDER BY grouped_streaks.trade_date) - 1) AS days_since_signal_cross,
    grouped_streaks.market_zone,
    (row_number() OVER (PARTITION BY grouped_streaks.symbol, grouped_streaks.grp_zone ORDER BY grouped_streaks.trade_date) - 1) AS days_in_current_zone,
    dense_rank() OVER (PARTITION BY grouped_streaks.trade_date ORDER BY grouped_streaks.hist_accel_pct_12 DESC NULLS LAST) AS rank_hist_accel_12,
    dense_rank() OVER (PARTITION BY grouped_streaks.trade_date ORDER BY grouped_streaks.hist_accel_pct_19 DESC NULLS LAST) AS rank_hist_accel_19,
        CASE
            WHEN ((grouped_streaks.hist_accel_pct_12 > (0)::numeric) AND ((grouped_streaks.volume)::numeric >= (grouped_streaks.volume_ema20 * 2.0))) THEN 'EXPLOSIVE_VOLUME_SURGE'::text
            WHEN ((grouped_streaks.hist_accel_pct_12 > (0)::numeric) AND ((grouped_streaks.volume)::numeric >= grouped_streaks.volume_ema20)) THEN 'CONFIRMED_BY_VOLUME'::text
            WHEN ((grouped_streaks.hist_accel_pct_12 > (0)::numeric) AND ((grouped_streaks.volume)::numeric < grouped_streaks.volume_ema20)) THEN 'LOW_VOLUME_WARNING'::text
            WHEN ((grouped_streaks.hist_accel_pct_12 < (0)::numeric) AND ((grouped_streaks.volume)::numeric >= grouped_streaks.volume_ema20)) THEN 'HIGH_VOLUME_SELLOFF'::text
            ELSE 'NEUTRAL'::text
        END AS signal_validation_status,
        CASE
            WHEN (grouped_streaks.macd_12_26_9 >= (0)::numeric) THEN 'ABOVE_ZERO'::text
            ELSE 'BELOW_ZERO'::text
        END AS macd_12_zero_status,
        CASE
            WHEN ((grouped_streaks.macd_12_26_9_hist > (0)::numeric) AND (grouped_streaks.macd_19_39_9_hist > (0)::numeric)) THEN 'DUAL_BULLISH'::text
            WHEN ((grouped_streaks.macd_12_26_9_hist < (0)::numeric) AND (grouped_streaks.macd_19_39_9_hist < (0)::numeric)) THEN 'DUAL_BEARISH'::text
            ELSE 'MIXED_DIVERGENT'::text
        END AS dual_macd_confluence,
        CASE
            WHEN ((grouped_streaks.prev_hist_12 < (0)::numeric) AND (grouped_streaks.macd_12_26_9_hist < (0)::numeric) AND (grouped_streaks.macd_12_26_9_hist > grouped_streaks.prev_hist_12)) THEN 'HIST_BOTTOMING_OUT'::text
            WHEN ((grouped_streaks.prev_hist_12 > (0)::numeric) AND (grouped_streaks.macd_12_26_9_hist > (0)::numeric) AND (grouped_streaks.macd_12_26_9_hist < grouped_streaks.prev_hist_12)) THEN 'HIST_PEAKING_OUT'::text
            ELSE 'NORMAL'::text
        END AS hist_reversal_signal
   FROM grouped_streaks;


ALTER VIEW public.v_macd_advanced_analysis OWNER TO "AdminKeng";

--
-- TOC entry 3636 (class 0 OID 0)
-- Dependencies: 253
-- Name: COLUMN v_macd_advanced_analysis.macd_12_zero_status; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.v_macd_advanced_analysis.macd_12_zero_status IS '(Trend Regime Filter): ระบุสภาวะตลาดใหญ่ สัญญาณ Golden Cross ที่เกิดขึ้น เหนือเส้น 0 จะเป็นสัญญาณ Follow-trend ที่แข็งแกร่งกว่า Golden Cross ที่เกิด ใต้เส้น 0 ซึ่งเป็นเพียงการดีดตัวระยะสั้น (Rebound)';


--
-- TOC entry 3637 (class 0 OID 0)
-- Dependencies: 253
-- Name: COLUMN v_macd_advanced_analysis.dual_macd_confluence; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.v_macd_advanced_analysis.dual_macd_confluence IS 'Fast vs Slow Multi-Trend): ตรวจเช็คว่าชุดเร็ว (12, 26, 9) และชุดช้า (19, 39, 9) ชี้ไปในทิศทางเดียวกันหรือไม่ หากเป็น DUAL_BULLISH จะกรองสัญญาณ False Signal ได้ดีกว่าดูชุดเดียว';


--
-- TOC entry 3638 (class 0 OID 0)
-- Dependencies: 253
-- Name: COLUMN v_macd_advanced_analysis.hist_reversal_signal; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.v_macd_advanced_analysis.hist_reversal_signal IS '(Histogram Momentum Deceleration): จับจุดที่ Histogram เริ่มหดตัวก่อนเกิด Signal Cross จริง เช่น ในโซนลบถ้า Histogram เริ่มหดสั้นลง (HIST_BOTTOMING_OUT) มักเป็นสัญญาณบอกล่วงหน้าว่าแรงขายเริ่มหมด';


--
-- TOC entry 254 (class 1259 OID 187893)
-- Name: v_bot_buy_opportunities; Type: VIEW; Schema: public; Owner: AdminKeng
--

CREATE VIEW public.v_bot_buy_opportunities AS
 WITH latest_date AS (
         SELECT max(v_macd_advanced_analysis.trade_date) AS max_date
           FROM public.v_macd_advanced_analysis
        ), macd_candidates AS (
         SELECT m.symbol,
            m.trade_date,
            m.close AS trigger_price,
            m.volume,
            m.vol_surge_ratio,
            m.macd_12_26_9_hist_accel_pct,
            m.market_zone,
            m.signal_validation_status,
            m.last_signal_cross_type,
            m.days_since_signal_cross,
            m.dual_macd_confluence,
            i.atr14,
            'MACD_MOMENTUM'::text AS strategy_name,
                CASE
                    WHEN ((m.signal_validation_status = 'EXPLOSIVE_VOLUME_SURGE'::text) AND (m.dual_macd_confluence = 'DUAL_BULLISH'::text)) THEN 5
                    WHEN ((m.signal_validation_status = 'CONFIRMED_BY_VOLUME'::text) AND (m.days_since_signal_cross <= 2) AND (m.last_signal_cross_type = 'GOLDEN_CROSS'::text)) THEN 4
                    WHEN ((m.signal_validation_status = 'CONFIRMED_BY_VOLUME'::text) AND (m.macd_12_26_9_hist_accel_pct > (0)::numeric)) THEN 3
                    ELSE 2
                END AS priority,
            format('Zone: %s | Accel: +%s%% | Vol: %sx (%s) | Confluence: %s'::text, m.market_zone, round(m.macd_12_26_9_hist_accel_pct, 2), m.vol_surge_ratio, m.signal_validation_status, m.dual_macd_confluence) AS reason
           FROM ((public.v_macd_advanced_analysis m
             JOIN latest_date ld ON ((m.trade_date = ld.max_date)))
             LEFT JOIN public.mv_stock_indicators i ON (((m.symbol = i.symbol) AND (m.trade_date = i.trade_date))))
          WHERE ((m.macd_12_26_9_hist_accel_pct > (0)::numeric) AND (m.signal_validation_status = ANY (ARRAY['CONFIRMED_BY_VOLUME'::text, 'EXPLOSIVE_VOLUME_SURGE'::text])) AND (m.market_zone = ANY (ARRAY['BUY_ZONE'::text, 'STRONG_BUY_ZONE'::text])))
        )
 SELECT macd_candidates.symbol,
    macd_candidates.trade_date,
    macd_candidates.trigger_price,
    macd_candidates.volume,
    macd_candidates.vol_surge_ratio,
    macd_candidates.macd_12_26_9_hist_accel_pct,
    macd_candidates.market_zone,
    macd_candidates.signal_validation_status,
    macd_candidates.last_signal_cross_type,
    macd_candidates.days_since_signal_cross,
    macd_candidates.dual_macd_confluence,
    macd_candidates.atr14,
    macd_candidates.strategy_name,
    macd_candidates.priority,
    macd_candidates.reason
   FROM macd_candidates
  ORDER BY macd_candidates.priority DESC, macd_candidates.macd_12_26_9_hist_accel_pct DESC;


ALTER VIEW public.v_bot_buy_opportunities OWNER TO "AdminKeng";

--
-- TOC entry 255 (class 1259 OID 188316)
-- Name: v_bot_sell_triggers; Type: VIEW; Schema: public; Owner: AdminKeng
--

CREATE VIEW public.v_bot_sell_triggers AS
 WITH latest_price AS (
         SELECT t.symbol,
            t.close AS current_price,
            t.date AS price_date
           FROM ( SELECT stock_price_history.symbol,
                    stock_price_history.close,
                    stock_price_history.date,
                    row_number() OVER (PARTITION BY stock_price_history.symbol ORDER BY stock_price_history.date DESC) AS rn
                   FROM public.stock_price_history) t
          WHERE (t.rn = 1)
        ), latest_signal AS (
         SELECT stock_signal.symbol,
            stock_signal.signal_type,
            stock_signal.trade_date
           FROM public.stock_signal
          WHERE (stock_signal.trade_date = ( SELECT max(stock_signal_1.trade_date) AS max
                   FROM public.stock_signal stock_signal_1))
        )
 SELECT b.id AS position_id,
    b.symbol,
    b.current_volume,
    b.entry_price,
    b.initial_stop_loss,
    b.trailing_stop_loss,
    b.max_price_reached,
    COALESCE(p.current_price, b.entry_price) AS market_price,
    round((((COALESCE(p.current_price, b.entry_price) - b.entry_price) / b.entry_price) * (100)::numeric), 2) AS percent_profit,
    s.signal_type,
        CASE
            WHEN ((((COALESCE(p.current_price, b.entry_price) - b.entry_price) / b.entry_price) * (100)::numeric) <= '-10.0'::numeric) THEN 'HARD_CUT_LOSS'::text
            WHEN (COALESCE(p.current_price, b.entry_price) <= b.initial_stop_loss) THEN 'INITIAL_SL_HIT'::text
            WHEN ((b.trailing_stop_loss IS NOT NULL) AND (COALESCE(p.current_price, b.entry_price) <= b.trailing_stop_loss)) THEN 'TRAILING_STOP_HIT'::text
            WHEN (s.signal_type = ANY (ARRAY['SELL'::text, 'SELL-STRONG'::text])) THEN 'TECHNICAL_SELL_SIGNAL'::text
            ELSE NULL::text
        END AS exit_trigger_type,
        CASE
            WHEN ((((COALESCE(p.current_price, b.entry_price) - b.entry_price) / b.entry_price) * (100)::numeric) <= '-10.0'::numeric) THEN 5
            WHEN (COALESCE(p.current_price, b.entry_price) <= b.initial_stop_loss) THEN 4
            WHEN ((b.trailing_stop_loss IS NOT NULL) AND (COALESCE(p.current_price, b.entry_price) <= b.trailing_stop_loss)) THEN 3
            WHEN (s.signal_type = ANY (ARRAY['SELL'::text, 'SELL-STRONG'::text])) THEN 2
            ELSE 0
        END AS exit_urgency_priority
   FROM ((public.bot_active_positions b
     LEFT JOIN latest_price p ON (((b.symbol)::text = (p.symbol)::text)))
     LEFT JOIN latest_signal s ON (((b.symbol)::text = s.symbol)))
  WHERE (((b.status)::text = 'OPEN'::text) AND (((((COALESCE(p.current_price, b.entry_price) - b.entry_price) / b.entry_price) * (100)::numeric) <= '-10.0'::numeric) OR (COALESCE(p.current_price, b.entry_price) <= b.initial_stop_loss) OR ((b.trailing_stop_loss IS NOT NULL) AND (COALESCE(p.current_price, b.entry_price) <= b.trailing_stop_loss)) OR (s.signal_type = ANY (ARRAY['SELL'::text, 'SELL-STRONG'::text]))));


ALTER VIEW public.v_bot_sell_triggers OWNER TO "AdminKeng";

--
-- TOC entry 3639 (class 0 OID 0)
-- Dependencies: 255
-- Name: VIEW v_bot_sell_triggers; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON VIEW public.v_bot_sell_triggers IS 'ตรวจจับเงื่อนไขการตัดขาย (database/v_bot_sell_triggers.sql)
ปรับ View ให้ตรวจเช็คสถานะหุ้นทั้งหมดที่เปิดอยู่ใน bot_active_positions คู่กับราคาล่าสุดจาก stock_price_history';


--
-- TOC entry 3640 (class 0 OID 0)
-- Dependencies: 255
-- Name: COLUMN v_bot_sell_triggers.exit_trigger_type; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON COLUMN public.v_bot_sell_triggers.exit_trigger_type IS 'ระบุเงื่อนไขการตัดขาย';


--
-- TOC entry 240 (class 1259 OID 16545)
-- Name: v_hybrid_stock_selection; Type: VIEW; Schema: public; Owner: AdminKeng
--

CREATE VIEW public.v_hybrid_stock_selection AS
 SELECT s.symbol,
    s.trade_date,
    s.signal_type,
    s.priority,
    s.reason AS technical_reason,
    v.value_score,
    v.rank AS fundamental_rank,
    p.close AS last_price
   FROM ((public.stock_signal s
     JOIN public.stock_value_score v ON ((s.symbol = v.name)))
     JOIN public.stock_price_history p ON (((s.symbol = (p.symbol)::text) AND (s.trade_date = p.date))))
  WHERE ((v.value_score >= (0.5)::double precision) AND (s.signal_type = ANY (ARRAY['BUY'::text, 'BUY-STRONG'::text])) AND (s.symbol IN ( SELECT settrade_stocklist.symbol
           FROM public.settrade_stocklist)) AND (s.trade_date IN ( SELECT DISTINCT stock_signal_1.trade_date
           FROM public.stock_signal stock_signal_1
          ORDER BY stock_signal_1.trade_date DESC
         LIMIT 3)))
  ORDER BY s.trade_date DESC, v.value_score DESC;


ALTER VIEW public.v_hybrid_stock_selection OWNER TO "AdminKeng";

--
-- TOC entry 3641 (class 0 OID 0)
-- Dependencies: 240
-- Name: VIEW v_hybrid_stock_selection; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON VIEW public.v_hybrid_stock_selection IS 'คัดมาเฉพาะ หุ้นที่ มีคุณค่า และกำลัง ขึ้นแรง';


--
-- TOC entry 252 (class 1259 OID 187860)
-- Name: v_portfolio_with_signals; Type: VIEW; Schema: public; Owner: AdminKeng
--

CREATE VIEW public.v_portfolio_with_signals AS
 WITH latest_port AS (
         SELECT portfolio_stock.account_no,
            portfolio_stock.imported_at,
            portfolio_stock.symbol,
            portfolio_stock.flag,
            portfolio_stock.nvdr_flag,
            portfolio_stock.market_price,
            portfolio_stock.amount,
            portfolio_stock.market_description,
            portfolio_stock.market_value,
            portfolio_stock.profit,
            portfolio_stock.percent_profit,
            portfolio_stock.realize_profit,
            portfolio_stock.start_volume,
            portfolio_stock.current_volume,
            portfolio_stock.actual_volume,
            portfolio_stock.start_price,
            portfolio_stock.average_price,
            portfolio_stock.show_na,
            portfolio_stock.port_flag,
            portfolio_stock.margin_rate,
            portfolio_stock.liabilities,
            portfolio_stock.commission_rate,
            portfolio_stock.vat_rate
           FROM public.portfolio_stock
          WHERE ((portfolio_stock.imported_at)::text = ( SELECT max((portfolio_stock_1.imported_at)::text) AS max
                   FROM public.portfolio_stock portfolio_stock_1))
        ), latest_signal AS (
         SELECT stock_signal.symbol,
            stock_signal.trade_date,
            stock_signal.signal_type,
            stock_signal.priority,
            stock_signal.reason,
            stock_signal.created_at
           FROM public.stock_signal
          WHERE (stock_signal.trade_date = ( SELECT max(stock_signal_1.trade_date) AS max
                   FROM public.stock_signal stock_signal_1))
        )
 SELECT p.account_no,
    p.symbol,
    c.quote_type,
    p.current_volume,
    p.average_price,
    p.market_price,
    p.profit,
    p.percent_profit,
    s.signal_type,
    s.reason AS signal_reason,
    s.trade_date AS signal_date,
    i.atr14,
    i.trend_status,
    COALESCE(b.is_managed_by_bot, false) AS is_managed_by_bot,
    b.entry_date AS bot_entry_date,
    b.entry_price AS bot_entry_price,
    b.initial_stop_loss,
    b.max_price_reached,
    b.trailing_stop_loss,
        CASE
            WHEN ((b.initial_stop_loss IS NOT NULL) AND (p.market_price > (0)::double precision)) THEN round(((((p.market_price - (b.initial_stop_loss)::double precision) / p.market_price) * (100)::double precision))::numeric, 2)
            ELSE NULL::numeric
        END AS buffer_to_initial_sl_pct
   FROM ((((latest_port p
     LEFT JOIN public.master_stock_classification c ON (((p.symbol)::text = (c.symbol)::text)))
     LEFT JOIN public.bot_active_positions b ON ((((p.symbol)::text = (b.symbol)::text) AND ((b.status)::text = 'OPEN'::text))))
     LEFT JOIN latest_signal s ON (((p.symbol)::text = s.symbol)))
     LEFT JOIN public.mv_stock_indicators i ON ((((p.symbol)::text = i.symbol) AND (s.trade_date = i.trade_date))))
  ORDER BY p.percent_profit DESC;


ALTER VIEW public.v_portfolio_with_signals OWNER TO "AdminKeng";

--
-- TOC entry 237 (class 1259 OID 16464)
-- Name: v_signal_last3day; Type: VIEW; Schema: public; Owner: AdminKeng
--

CREATE VIEW public.v_signal_last3day AS
 SELECT stock_signal.trade_date,
    stock_signal.signal_type,
    count(*) AS count
   FROM public.stock_signal
  WHERE (stock_signal.trade_date IN ( SELECT DISTINCT stock_signal_1.trade_date
           FROM public.stock_signal stock_signal_1
          ORDER BY stock_signal_1.trade_date DESC
         LIMIT 3))
  GROUP BY stock_signal.signal_type, stock_signal.trade_date
  ORDER BY stock_signal.trade_date DESC, stock_signal.signal_type;


ALTER VIEW public.v_signal_last3day OWNER TO "AdminKeng";

--
-- TOC entry 239 (class 1259 OID 16538)
-- Name: v_stock_indicators_bk_oldVer; Type: VIEW; Schema: public; Owner: AdminKeng
--

CREATE VIEW public."v_stock_indicators_bk_oldVer" AS
 SELECT stock_indicator_jsonb.symbol,
    stock_indicator_jsonb.trade_date,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'atr14'::text), 'null'::text))::numeric AS atr14,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'bb_lower'::text), 'null'::text))::numeric AS bb_lower,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'bb_mid'::text), 'null'::text))::numeric AS bb_mid,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'bb_upper'::text), 'null'::text))::numeric AS bb_upper,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema10'::text), 'null'::text))::numeric AS ema10,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema12'::text), 'null'::text))::numeric AS ema12,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema20'::text), 'null'::text))::numeric AS ema20,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema200'::text), 'null'::text))::numeric AS ema200,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema26'::text), 'null'::text))::numeric AS ema26,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema5'::text), 'null'::text))::numeric AS ema5,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'ema50'::text), 'null'::text))::numeric AS ema50,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_12_26_9'::text), 'null'::text))::numeric AS macd_12_26_9,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_12_26_9_hist'::text), 'null'::text))::numeric AS macd_12_26_9_hist,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_12_26_9_signal'::text), 'null'::text))::numeric AS macd_12_26_9_signal,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_19_39_9'::text), 'null'::text))::numeric AS macd_19_39_9,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_19_39_9_hist'::text), 'null'::text))::numeric AS macd_19_39_9_hist,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'macd_19_39_9_signal'::text), 'null'::text))::numeric AS macd_19_39_9_signal,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'rsi14'::text), 'null'::text))::numeric AS rsi14,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'rsi21'::text), 'null'::text))::numeric AS rsi21,
    NULLIF((stock_indicator_jsonb.indicators ->> 'trend_status'::text), 'null'::text) AS trend_status,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'volume_avg20'::text), 'null'::text))::numeric AS volume_avg20,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'volume_ema20'::text), 'null'::text))::numeric AS volume_ema20,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'volume_ema5'::text), 'null'::text))::numeric AS volume_ema5,
    (NULLIF((stock_indicator_jsonb.indicators ->> 'volume_ema50'::text), 'null'::text))::numeric AS volume_ema50
   FROM public.stock_indicator_jsonb;


ALTER VIEW public."v_stock_indicators_bk_oldVer" OWNER TO "AdminKeng";

--
-- TOC entry 3406 (class 2604 OID 183692)
-- Name: account_info_history id; Type: DEFAULT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.account_info_history ALTER COLUMN id SET DEFAULT nextval('public.account_info_history_id_seq'::regclass);


--
-- TOC entry 3392 (class 2604 OID 179689)
-- Name: bot_active_positions id; Type: DEFAULT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.bot_active_positions ALTER COLUMN id SET DEFAULT nextval('public.bot_active_positions_id_seq'::regclass);


--
-- TOC entry 3402 (class 2604 OID 179716)
-- Name: bot_orders order_id; Type: DEFAULT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.bot_orders ALTER COLUMN order_id SET DEFAULT nextval('public.bot_orders_order_id_seq'::regclass);


--
-- TOC entry 3398 (class 2604 OID 179704)
-- Name: bot_trade_signals id; Type: DEFAULT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.bot_trade_signals ALTER COLUMN id SET DEFAULT nextval('public.bot_trade_signals_id_seq'::regclass);


--
-- TOC entry 3387 (class 2604 OID 16473)
-- Name: stock_prices id; Type: DEFAULT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stock_prices ALTER COLUMN id SET DEFAULT nextval('public.stock_prices_id_seq'::regclass);


--
-- TOC entry 3390 (class 2604 OID 16474)
-- Name: stocklist_twelvedata id; Type: DEFAULT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stocklist_twelvedata ALTER COLUMN id SET DEFAULT nextval('public.stocklist_twelvedata_id_seq'::regclass);


--
-- TOC entry 3451 (class 2606 OID 183696)
-- Name: account_info_history account_info_history_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.account_info_history
    ADD CONSTRAINT account_info_history_pkey PRIMARY KEY (id);


--
-- TOC entry 3442 (class 2606 OID 179698)
-- Name: bot_active_positions bot_active_positions_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.bot_active_positions
    ADD CONSTRAINT bot_active_positions_pkey PRIMARY KEY (id);


--
-- TOC entry 3448 (class 2606 OID 179723)
-- Name: bot_orders bot_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.bot_orders
    ADD CONSTRAINT bot_orders_pkey PRIMARY KEY (order_id);


--
-- TOC entry 3445 (class 2606 OID 179710)
-- Name: bot_trade_signals bot_trade_signals_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.bot_trade_signals
    ADD CONSTRAINT bot_trade_signals_pkey PRIMARY KEY (id);


--
-- TOC entry 3410 (class 2606 OID 16482)
-- Name: dim_symbol_th dim_symbol_th_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.dim_symbol_th
    ADD CONSTRAINT dim_symbol_th_pkey PRIMARY KEY (symbol, exchange);


--
-- TOC entry 3440 (class 2606 OID 175856)
-- Name: master_stock_classification master_stock_classification_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.master_stock_classification
    ADD CONSTRAINT master_stock_classification_pkey PRIMARY KEY (symbol);


--
-- TOC entry 3414 (class 2606 OID 16484)
-- Name: stock_indicator_daily stock_indicator_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stock_indicator_daily
    ADD CONSTRAINT stock_indicator_daily_pkey PRIMARY KEY (symbol, trade_date);


--
-- TOC entry 3417 (class 2606 OID 16486)
-- Name: stock_indicator_daily_v4 stock_indicator_daily_v4_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stock_indicator_daily_v4
    ADD CONSTRAINT stock_indicator_daily_v4_pkey PRIMARY KEY (symbol, trade_date);


--
-- TOC entry 3420 (class 2606 OID 16488)
-- Name: stock_indicator_jsonb stock_indicator_jsonb_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stock_indicator_jsonb
    ADD CONSTRAINT stock_indicator_jsonb_pkey PRIMARY KEY (symbol, trade_date);


--
-- TOC entry 3424 (class 2606 OID 16490)
-- Name: stock_price_history stock_price_history_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stock_price_history
    ADD CONSTRAINT stock_price_history_pkey PRIMARY KEY (symbol, date);


--
-- TOC entry 3427 (class 2606 OID 16493)
-- Name: stock_prices stock_prices_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stock_prices
    ADD CONSTRAINT stock_prices_pkey PRIMARY KEY (id);


--
-- TOC entry 3430 (class 2606 OID 16495)
-- Name: stock_signal stock_signal_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stock_signal
    ADD CONSTRAINT stock_signal_pkey PRIMARY KEY (symbol, trade_date);


--
-- TOC entry 3432 (class 2606 OID 16497)
-- Name: stocklist_twelvedata stocklist_twelvedata_pkey; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stocklist_twelvedata
    ADD CONSTRAINT stocklist_twelvedata_pkey PRIMARY KEY (id);


--
-- TOC entry 3434 (class 2606 OID 16499)
-- Name: stocklist_twelvedata stocklist_twelvedata_symbol_key; Type: CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.stocklist_twelvedata
    ADD CONSTRAINT stocklist_twelvedata_symbol_key UNIQUE (symbol);


--
-- TOC entry 3452 (class 1259 OID 183697)
-- Name: idx_account_active; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX idx_account_active ON public.account_info_history USING btree (account_no, is_disabled);


--
-- TOC entry 3443 (class 1259 OID 179699)
-- Name: idx_bot_active_positions_open; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE UNIQUE INDEX idx_bot_active_positions_open ON public.bot_active_positions USING btree (symbol) WHERE ((status)::text = 'OPEN'::text);


--
-- TOC entry 3642 (class 0 OID 0)
-- Dependencies: 3443
-- Name: INDEX idx_bot_active_positions_open; Type: COMMENT; Schema: public; Owner: AdminKeng
--

COMMENT ON INDEX public.idx_bot_active_positions_open IS 'ป้องกันการเปิด Position ซ้ำซ้อนของหุ้นตัวเดียวกันขณะสถานะยัง OPEN';


--
-- TOC entry 3449 (class 1259 OID 179729)
-- Name: idx_bot_orders_symbol_status; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX idx_bot_orders_symbol_status ON public.bot_orders USING btree (symbol, status);


--
-- TOC entry 3446 (class 1259 OID 179711)
-- Name: idx_bot_trade_signals_status; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX idx_bot_trade_signals_status ON public.bot_trade_signals USING btree (status, trade_date);


--
-- TOC entry 3453 (class 1259 OID 187598)
-- Name: idx_mv_stock_ind_date; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX idx_mv_stock_ind_date ON public.mv_stock_indicators USING btree (trade_date);


--
-- TOC entry 3454 (class 1259 OID 187597)
-- Name: idx_mv_stock_ind_sym_date; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE UNIQUE INDEX idx_mv_stock_ind_sym_date ON public.mv_stock_indicators USING btree (symbol, trade_date);


--
-- TOC entry 3421 (class 1259 OID 16543)
-- Name: idx_stock_price_history_date; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX idx_stock_price_history_date ON public.stock_price_history USING btree (date);


--
-- TOC entry 3422 (class 1259 OID 16544)
-- Name: idx_stock_price_history_sym_date; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX idx_stock_price_history_sym_date ON public.stock_price_history USING btree (symbol, date);


--
-- TOC entry 3425 (class 1259 OID 16500)
-- Name: idx_stock_prices_symbol_time; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX idx_stock_prices_symbol_time ON public.stock_prices USING btree (symbol, price_time);


--
-- TOC entry 3435 (class 1259 OID 122222)
-- Name: idx_stock_value_score_name; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX idx_stock_value_score_name ON public.stock_value_score USING btree (name);


--
-- TOC entry 3436 (class 1259 OID 122221)
-- Name: idx_stock_value_score_rank; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX idx_stock_value_score_rank ON public.stock_value_score USING btree (rank);


--
-- TOC entry 3411 (class 1259 OID 16501)
-- Name: ix_dim_symbol_th_market; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX ix_dim_symbol_th_market ON public.dim_symbol_th USING btree (market);


--
-- TOC entry 3412 (class 1259 OID 16502)
-- Name: ix_stock_indicator_daily_symdate; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX ix_stock_indicator_daily_symdate ON public.stock_indicator_daily USING btree (symbol, trade_date);


--
-- TOC entry 3415 (class 1259 OID 16503)
-- Name: ix_stock_indicator_daily_v4_symdate; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX ix_stock_indicator_daily_v4_symdate ON public.stock_indicator_daily_v4 USING btree (symbol, trade_date);


--
-- TOC entry 3418 (class 1259 OID 16504)
-- Name: ix_stock_indicator_jsonb_symdate; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX ix_stock_indicator_jsonb_symdate ON public.stock_indicator_jsonb USING btree (symbol, trade_date);


--
-- TOC entry 3428 (class 1259 OID 16505)
-- Name: ix_stock_signal_symdate; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX ix_stock_signal_symdate ON public.stock_signal USING btree (symbol, trade_date);


--
-- TOC entry 3437 (class 1259 OID 16516)
-- Name: stock_value_score_name_idx; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX stock_value_score_name_idx ON public.stock_value_score USING btree (name);


--
-- TOC entry 3438 (class 1259 OID 16515)
-- Name: stock_value_score_rank_idx; Type: INDEX; Schema: public; Owner: AdminKeng
--

CREATE INDEX stock_value_score_rank_idx ON public.stock_value_score USING btree (rank);


--
-- TOC entry 3455 (class 2606 OID 179724)
-- Name: bot_orders bot_orders_signal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: AdminKeng
--

ALTER TABLE ONLY public.bot_orders
    ADD CONSTRAINT bot_orders_signal_id_fkey FOREIGN KEY (signal_id) REFERENCES public.bot_trade_signals(id);


-- Completed on 2026-09-04 13:32:57

--
-- PostgreSQL database dump complete
--

\unrestrict MtS7JTljYPw77Z5uHcpSeQSYtMzCEObAJV9WifiveLHNi4roHzrm2MNCO5g7neO

