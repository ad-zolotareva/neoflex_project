CREATE SEQUENCE IF NOT EXISTS dm.seq_lg_messages
    INCREMENT 1
    START 101
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

-- DROP TABLE IF EXISTS dm.lg_messages;
CREATE TABLE IF NOT EXISTS dm.lg_messages
(
    record_id integer,
    date_time timestamp without time zone,
    pid integer,
    message text,
    message_type integer,
    usename name,
    datname name,
    client_addr inet,
    application_name text,
    backend_start timestamp with time zone
);


--1.2.1
-- DROP TABLE IF EXISTS dm.dm_account_turnover_f;
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f
(
    on_date date,
    account_rk numeric,
    credit_amount numeric(23,8),
    credit_amount_rub numeric(23,8),
    debet_amount numeric(23,8),
    debet_amount_rub numeric(23,8)
)

--процедура для расчета оборотов на каждый день января 2018 года
CREATE OR REPLACE PROCEDURE dm.my_procedure(
	IN date_start date,
	IN date_end date)
LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
currentDate date;
BEGIN
FOR currentDate IN (SELECT * FROM generate_series(date_start, date_end, '1 day') s(currentDate)) LOOP
	call ds.fill_account_turnover_f(currentDate);
END LOOP;
END;
$BODY$;


--1.2.2
-- DROP TABLE IF EXISTS dm.dm_f101_round_f;
CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f
(
    from_date date,
    to_date date,
    chapter character varying(1),
    ledger_account character varying(5),
    characteristic character varying(1),
    balance_in_rub numeric(23,8),
    r_balance_in_rub numeric(23,8),
    balance_in_val numeric(23,8),
    r_balance_in_val numeric(23,8),
    balance_in_total numeric(23,8),
    r_balance_in_total numeric(23,8),
    turn_deb_rub numeric(23,8),
    r_turn_deb_rub numeric(23,8),
    turn_deb_val numeric(23,8),
    r_turn_deb_val numeric(23,8),
    turn_deb_total numeric(23,8),
    r_turn_deb_total numeric(23,8),
    turn_cre_rub numeric(23,8),
    r_turn_cre_rub numeric(23,8),
    turn_cre_val numeric(23,8),
    r_turn_cre_val numeric(23,8),
    turn_cre_total numeric(23,8),
    r_turn_cre_total numeric(23,8),
    balance_out_rub numeric(23,8),
    r_balance_out_rub numeric(23,8),
    balance_out_val numeric(23,8),
    r_balance_out_val numeric(23,8),
    balance_out_total numeric(23,8),
    r_balance_out_total numeric(23,8)
);
