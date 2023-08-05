DROP DATABASE IF EXISTS part4;
CREATE DATABASE part4 ENCODING 'UTF8';

DROP TABLE if EXISTS persons;
CREATE TABLE persons
    (
        nickname varchar
            PRIMARY KEY,
        birthday date
    );

DROP TABLE if EXISTS tablename_1;
CREATE TABLE tablename_1
    (
        id serial
            PRIMARY KEY,
        peer varchar,
        task varchar
    );

DROP TABLE if EXISTS tablename_2;
CREATE TABLE tablename_2
    (
        id serial
            PRIMARY KEY,
        peer varchar,
        task varchar
    );

DROP TABLE if EXISTS _3_tablename_3;
CREATE TABLE _3_tablename_3
    (
        id serial
            PRIMARY KEY,
        peer varchar,
        task varchar
    );

DROP FUNCTION if EXISTS add_foo(a INTEGER, b INTEGER);
CREATE FUNCTION add_foo(a INTEGER, b INTEGER) RETURNS integer AS $$
BEGIN
RETURN a + b;
END;
$$
LANGUAGE plpgsql;

DROP FUNCTION if EXISTS sub_foo(a INTEGER, b INTEGER);
CREATE FUNCTION sub_foo(a INTEGER, b INTEGER) RETURNS integer AS $$
BEGIN
RETURN a - b;
END;
$$
LANGUAGE plpgsql;

DROP FUNCTION if EXISTS mul_foo();
CREATE FUNCTION mul_foo() RETURNS integer AS $$
BEGIN
RETURN 10 * 3;
END;
$$
LANGUAGE plpgsql;

DROP FUNCTION if EXISTS test();
CREATE FUNCTION test() RETURNS trigger AS $$
BEGIN
RETURN NULL;
END
$$
LANGUAGE plpgsql;

DROP TRIGGER if EXISTS tr_part_4 ON persons;
CREATE TRIGGER tr_part_4
    AFTER INSERT
    ON persons
    FOR EACH ROW EXECUTE procedure test();

DROP TRIGGER if EXISTS tr_part_4_one ON persons;
CREATE TRIGGER tr_part_4_one
    AFTER UPDATE
    ON persons
    FOR EACH ROW EXECUTE procedure test();

---------------------------------------------------------------------------------------------
----------- 4.1 Создать хранимую процедуру, которая уничтожает таблицы 'TableName' ----------
---------------------------------------------------------------------------------------------

-- выведем список всех существующих таблиц
SELECT table_name
  FROM information_schema.tables
 WHERE table_schema NOT IN ('information_schema', 'pg_catalog');

DROP PROCEDURE if EXISTS pr_delete_table_with_tablename();
CREATE PROCEDURE pr_delete_table_with_tablename() AS
$$
DECLARE
r VARCHAR;
BEGIN
FOR r IN (SELECT TABLE_NAME
                FROM information_schema.tables
               WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                 AND TABLE_NAME ILIKE 'tablename%') LOOP
        EXECUTE FORMAT('drop table %I cascade', r);
END LOOP;
END;
$$
LANGUAGE plpgsql;

CALL pr_delete_table_with_tablename();

---------------------------------------------------------------------------------------------
-------------------- 4.2 Создать процедуру, которая выводит список функций ------------------
---------------------------------------------------------------------------------------------

DROP PROCEDURE if EXISTS pr_show_scalar_foo(IN res refcursor, OUT COUNT INT);
CREATE PROCEDURE pr_show_scalar_foo(IN res refcursor, OUT COUNT INT) AS
$$
BEGIN
OPEN res FOR (SELECT ROUTINE_NAME, STRING_AGG(p.parameter_name, ',') AS param
                    FROM information_schema.routines r
                             LEFT JOIN information_schema.parameters p
                             ON r.specific_name = p.specific_name
                   WHERE r.specific_schema NOT IN ('information_schema', 'pg_catalog')
                     AND routine_type = 'FUNCTION'
                     AND p.parameter_name IS NOT NULL
                   GROUP BY ROUTINE_NAME);
COUNT = COUNT(*)
             FROM (SELECT ROUTINE_NAME, STRING_AGG(p.parameter_name, ',') AS param
                     FROM information_schema.routines r
                              LEFT JOIN information_schema.parameters p
                              ON r.specific_name = p.specific_name
                    WHERE r.specific_schema NOT IN ('information_schema', 'pg_catalog')
                      AND routine_type = 'FUNCTION'
                      AND p.parameter_name IS NOT NULL
                    GROUP BY ROUTINE_NAME) AS q;
END;
$$
LANGUAGE plpgsql;

BEGIN;
CALL pr_show_scalar_foo('cursor', NULL);
FETCH all IN "cursor";
COMMIT;
END;

---------------------------------------------------------------------------------------------
--------------- 4.3 Создать процедуру, которая уничтожает все SQL DML триггеры  -------------
---------------------------------------------------------------------------------------------

-- выведем список всех триггеров
SELECT trigger_name AS n, event_object_table AS t
  FROM information_schema.triggers;

DROP PROCEDURE if EXISTS pr_delete_triggers(OUT COUNT INT);
CREATE PROCEDURE pr_delete_triggers(OUT COUNT INT) AS
$$
DECLARE
r record;
BEGIN
COUNT = 0;
FOR r IN
SELECT trigger_name AS n, event_object_table AS t
  FROM information_schema.triggers loop EXECUTE FORMAT('DROP TRIGGER %I ON %I', r.n, r.t);
COUNT = COUNT + 1;
END LOOP;
END;
$$
LANGUAGE plpgsql;

CALL pr_delete_triggers(NULL);

---------------------------------------------------------------------------------------------
----------- 4.4 Создать процедуру, которая выводит имена и описания типа объектов  ----------
---------------------------------------------------------------------------------------------

DROP PROCEDURE if EXISTS pr_show_names(INOUT res refcursor, IN mask VARCHAR);
CREATE PROCEDURE pr_show_names(INOUT res refcursor, IN mask VARCHAR) AS
$$
BEGIN
OPEN res FOR (SELECT ROUTINE_NAME, routine_type
                    FROM information_schema.routines
                   WHERE ROUTINE_SCHEMA NOT IN ('information_schema', 'pg_catalog')
                     AND routine_type IN ('FUNCTION', 'PROCEDURE')
                     AND routine_definition LIKE '%' || mask || '%');
END;
$$
LANGUAGE plpgsql;

BEGIN;
CALL pr_show_names('cur', 'DEC');
FETCH all IN "cur";
COMMIT;
END;
