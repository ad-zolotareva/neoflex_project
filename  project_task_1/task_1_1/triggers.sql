DROP TABLE IF EXISTS logs.log_table CASCADE;

--Таблица для логирования

--CREATE SCHEMA LOGS;

CREATE TABLE IF NOT EXISTS logs.log_table
(
    id_log_table serial NOT NULL,
    log_data text COLLATE pg_catalog."default" NOT NULL,
    log_time timestamp without time zone NOT NULL,
    CONSTRAINT logs_pk PRIMARY KEY (id_log_table)
)

CREATE TABLE IF NOT EXISTS LOGS.audit_table (
	id_audit_table serial,
	name_table text,
    old_row_data jsonb,
    new_row_data jsonb,
    dml_type TEXT NOT NULL,
    dml_timestamp timestamp NOT NULL,
    constraint PK_Audit PRIMARY KEY (id_audit_table )
) ;


CREATE OR REPLACE FUNCTION f_audit_trigger()
RETURNS trigger
AS
$body$
BEGIN
   if (TG_OP = 'INSERT')
   then INSERT INTO LOGS.audit_table (name_table, old_row_data, new_row_data, dml_type, dml_timestamp)
        VALUES(TG_TABLE_NAME, null, to_jsonb(NEW), 'INSERT', CURRENT_TIMESTAMP);
        RETURN NEW;

   elsif (TG_OP = 'UPDATE') then
       INSERT INTO LOGS.audit_table (name_table, old_row_data, new_row_data, dml_type, dml_timestamp)
       VALUES(TG_TABLE_NAME, to_jsonb(OLD), to_jsonb(NEW), 'UPDATE',  CURRENT_TIMESTAMP );
       RETURN NEW;

   elsif (TG_OP = 'DELETE') then
       INSERT INTO LOGS.audit_table (name_table, old_row_data, new_row_data, dml_type, dml_timestamp)
       VALUES(TG_TABLE_NAME, to_jsonb(OLD), null, 'DELETE', CURRENT_TIMESTAMP );
       RETURN OLD;
   end if;

END;
$body$
LANGUAGE plpgsql;

CREATE OR REPLACE  TRIGGER audit_trigger_balance
AFTER INSERT OR UPDATE OR DELETE
ON DS.FT_BALANCE_F
FOR EACH ROW
EXECUTE FUNCTION f_audit_trigger();

CREATE OR REPLACE  TRIGGER audit_trigger_posting
AFTER INSERT OR UPDATE OR DELETE
ON DS.FT_POSTING_F
FOR EACH ROW
EXECUTE FUNCTION f_audit_trigger();

CREATE OR REPLACE  TRIGGER audit_trigger_account
AFTER INSERT OR UPDATE OR DELETE
ON DS.MD_ACCOUNT_D
FOR EACH ROW
EXECUTE FUNCTION f_audit_trigger();

CREATE OR REPLACE  TRIGGER audit_trigger_currency
AFTER INSERT OR UPDATE OR DELETE
ON DS.MD_CURRENCY_D
FOR EACH ROW
EXECUTE FUNCTION f_audit_trigger();

CREATE OR REPLACE  TRIGGER audit_trigger_exchange_rate
AFTER INSERT OR UPDATE OR DELETE
ON DS.MD_EXCHANGE_RATE_D
FOR EACH ROW
EXECUTE FUNCTION f_audit_trigger();

CREATE OR REPLACE  TRIGGER audit_trigger_ledger
AFTER INSERT OR UPDATE OR DELETE
ON DS.MD_LEDGER_ACCOUNT_S
FOR EACH ROW
EXECUTE FUNCTION f_audit_trigger();