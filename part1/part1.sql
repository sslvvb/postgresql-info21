-- create database school21 encoding 'UTF8';

DROP TABLE IF EXISTS peers CASCADE;
CREATE TABLE peers
    (
        nickname varchar NOT NULL
            PRIMARY KEY
            UNIQUE,
        birthday date NOT NULL
    );

DROP TABLE IF EXISTS tasks CASCADE;
CREATE TABLE tasks
    (
        title varchar NOT NULL
            PRIMARY KEY
            UNIQUE,
        parenttask varchar,
        maxxp bigint NOT NULL
            CHECK ( maxxp > 0 ) DEFAULT 0,
        CONSTRAINT fk_tasks_parenttask
            FOREIGN KEY (parenttask) REFERENCES tasks (title)
    );

DROP TYPE IF EXISTS check_status CASCADE;
CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');

DROP TABLE IF EXISTS checks CASCADE;
CREATE TABLE checks
    (
        id serial
            PRIMARY KEY,
        peer varchar NOT NULL,
        task varchar NOT NULL,
        date date NOT NULL
            CHECK ( date <= CURRENT_DATE ) DEFAULT CURRENT_DATE,
        CONSTRAINT fk_checks_peer
            FOREIGN KEY (peer) REFERENCES peers (nickname),
        CONSTRAINT fk_checks_task
            FOREIGN KEY (task) REFERENCES tasks (title)
    );

DROP TABLE IF EXISTS p2p;
CREATE TABLE p2p
    (
        id serial
            PRIMARY KEY,
        "Check" serial,
        checkingpeer varchar NOT NULL,
        state check_status,
        time time DEFAULT CURRENT_TIME,
        CONSTRAINT fk_p2p_check
            FOREIGN KEY ("Check") REFERENCES checks (id),
        CONSTRAINT fk_p2p_checkingpeer
            FOREIGN KEY (checkingpeer) REFERENCES peers (nickname)
    );

DROP TABLE IF EXISTS verter;
CREATE TABLE verter
    (
        id serial
            PRIMARY KEY,
        "Check" serial,
        state check_status,
        time time DEFAULT CURRENT_TIME,
        CONSTRAINT fk_verter_check
            FOREIGN KEY ("Check") REFERENCES checks (id)
    );

DROP TABLE IF EXISTS transferredpoints;
CREATE TABLE transferredpoints
    (
        id serial
            PRIMARY KEY,
        checkingpeer varchar NOT NULL,
        checkedpeer varchar NOT NULL,
        pointsamount bigint NOT NULL,
        CONSTRAINT fk_transferredpoints_checkingpeer
            FOREIGN KEY (checkingpeer) REFERENCES peers (nickname),
        CONSTRAINT fk_transferredpoints_checkedpeer
            FOREIGN KEY (checkedpeer) REFERENCES peers (nickname)
    );

DROP TABLE IF EXISTS friends;
CREATE TABLE friends
    (
        id serial
            PRIMARY KEY,
        peer1 varchar NOT NULL,
        peer2 varchar NOT NULL,
        CONSTRAINT fk_friends_peer1
            FOREIGN KEY (peer1) REFERENCES peers (nickname),
        CONSTRAINT fk_friends_peer2
            FOREIGN KEY (peer2) REFERENCES peers (nickname),
        CONSTRAINT un_peers_unique
            UNIQUE (peer1, peer2)
    );

DROP TABLE IF EXISTS recommendations;
CREATE TABLE recommendations
    (
        id serial
            PRIMARY KEY,
        peer varchar NOT NULL,
        recommendedpeer varchar NOT NULL,
        CONSTRAINT fk_recommendations_peer
            FOREIGN KEY (peer) REFERENCES peers (nickname),
        CONSTRAINT fk_recommendations_recommendedpeer
            FOREIGN KEY (recommendedpeer) REFERENCES peers (nickname),
        CONSTRAINT un_peers_recommendetions_unique
            UNIQUE (peer, recommendedpeer)
    );

DROP TABLE IF EXISTS xp;
CREATE TABLE xp
    (
        id serial
            PRIMARY KEY,
        "Check" serial,
        xpamount bigint NOT NULL
            CHECK ( xpamount > 0 ),
        CONSTRAINT fk_xp_check
            FOREIGN KEY ("Check") REFERENCES checks (id)
    );

DROP TABLE IF EXISTS timetracking;
CREATE TABLE timetracking
    (
        id serial
            PRIMARY KEY,
        peer varchar NOT NULL,
        date date NOT NULL
            CHECK ( date <= CURRENT_DATE ),
        time time NOT NULL
            CHECK ( time <= current_time AND Date = current_date OR Date <= current_date ),
        state int
            CHECK ( state IN (1, 2) ),
        CONSTRAINT fk_timetracking
            FOREIGN KEY (peer) REFERENCES peers (nickname)
    );

DROP PROCEDURE IF EXISTS input_data(varchar);
CREATE PROCEDURE input_data(delim varchar) AS
$$
DECLARE
    path text DEFAULT ('/Users/sslvvb/Documents/rep/part1/import'); DECLARE dir varchar;
BEGIN
    dir := path || '/peers.csv';
    EXECUTE FORMAT('copy peers from %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/tasks.csv';
    EXECUTE FORMAT('copy tasks from %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/checks.csv';
    EXECUTE FORMAT('copy checks from %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/p2p.csv';
    EXECUTE FORMAT('copy p2p from %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/verter.csv';
    EXECUTE FORMAT('copy verter from %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/xp.csv';
    EXECUTE FORMAT('copy xp from %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/transferredpoints.csv';
    EXECUTE FORMAT('copy transferredpoints from %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/friends.csv';
    EXECUTE FORMAT('copy friends from %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/recommendations.csv';
    EXECUTE FORMAT('copy recommendations from %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/timetracking.csv';
    EXECUTE FORMAT('copy timetracking from %L with (format csv, header, delimiter %L);', dir, delim);
END
$$ LANGUAGE plpgsql;

CALL input_data(',');

drop procedure if exists output_data(varchar);
create procedure output_data(delim varchar)
AS $$
declare path text default ('/Users/sslvvb/Documents/rep/part1/');
declare dir varchar;
BEGIN
    dir := path || '/from_peers.csv';
    EXECUTE FORMAT('copy peers to %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/from_tasks.csv';
    EXECUTE FORMAT('copy tasks to %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/from_checks.csv';
    EXECUTE FORMAT('copy checks to %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/from_p2p.csv';
    EXECUTE FORMAT('copy p2p to %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/from_verter.csv';
    EXECUTE FORMAT('copy verter to %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/from_xp.csv';
    EXECUTE FORMAT('copy xp to %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/from_transferredpoints.csv';
    EXECUTE FORMAT('copy transferredpoints to %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/from_friends.csv';
    EXECUTE FORMAT('copy friends to %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/from_recommendations.csv';
    EXECUTE FORMAT('copy recommendations to %L with (format csv, header, delimiter %L);', dir, delim);
    dir := path || '/from_timetracking.csv';
    EXECUTE FORMAT('copy timetracking to %L with (format csv, header, delimiter %L);', dir, delim);
END
$$ LANGUAGE plpgsql;

CALL output_data(',');
