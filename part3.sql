------------------------------------ 1 --------------------------------------------
-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
DROP FUNCTION IF EXISTS part3_task1;
CREATE OR REPLACE FUNCTION part3_task1()
    RETURNS TABLE
                (
                    peer1 varchar,
                    peer2 varchar,
                    pointsamount bigint
                )
AS
$$
SELECT table1.checkingpeer AS peer1, table1.checkedpeer AS peer2,
       CASE WHEN (table1.pointsamount <= table2.pointsamount) THEN table1.pointsamount - table2.pointsamount
            WHEN (table1.pointsamount > table2.pointsamount) THEN table1.pointsamount - table2.pointsamount
            ELSE table1.pointsamount END
  FROM transferredpoints AS table1
           LEFT JOIN transferredpoints AS table2
           ON table1.checkingpeer = table2.checkedpeer AND table1.checkedpeer = table2.checkingpeer;
$$ LANGUAGE sql;

SELECT *
  FROM part3_task1();

------------------------------------ 2 --------------------------------------------
-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
DROP FUNCTION IF EXISTS part3_task2();
CREATE OR REPLACE FUNCTION part3_task2()
    RETURNS TABLE
                (
                    peer varchar,
                    task varchar,
                    xp bigint
                )
AS
$$
  WITH cte AS (SELECT peer AS peer, task AS task, xpamount AS xp
                 FROM checks
                          JOIN xp x
                          ON checks.id = x."Check"
                ORDER BY peer, task)
SELECT *
  FROM cte
$$ LANGUAGE sql;

SELECT *
  FROM part3_task2();

------------------------------------ 3 --------------------------------------------
-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
DROP FUNCTION IF EXISTS part3_task3(date);
CREATE OR REPLACE FUNCTION part3_task3(indate date DEFAULT CURRENT_DATE)
    RETURNS TABLE
                (
                    peer varchar
                )
AS
$$
  WITH cte AS (SELECT peer FROM timetracking WHERE date = indate GROUP BY peer HAVING COUNT(state) <= 2)
SELECT *
  FROM cte
$$ LANGUAGE sql;

SELECT *
  FROM part3_task3('2023-01-22');

------------------------------------ 4 --------------------------------------------
-- 4) Найти процент успешных и неуспешных проверок за всё время
DROP PROCEDURE IF EXISTS part3_task4(INOUT res refcursor);
CREATE OR REPLACE PROCEDURE part3_task4(INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR SELECT ROUND(success * 100 / total, 2) AS successfulchecks,
                        100 - ROUND(success * 100 / total, 2) AS unsuccessfulchecks
                   FROM (SELECT COUNT(peer) AS total
                           FROM checks
                                    FULL JOIN verter v
                                    ON checks.id = v."Check"
                                    FULL JOIN p2p p
                                    ON checks.id = p."Check"
                          WHERE (v.state IN ('Success', 'Failure') OR v.state IS NULL)
                            AND p.state IN ('Success', 'Failure')) AS a,

                        (SELECT COUNT(peer) AS success
                           FROM checks
                                    FULL JOIN verter v
                                    ON checks.id = v."Check"
                                    FULL JOIN p2p p
                                    ON checks.id = p."Check"
                          WHERE (v.state = 'Success' OR v.state IS NULL)
                            AND p.state = 'Success') AS b,

                        (SELECT COUNT(peer) AS failure
                           FROM checks
                                    FULL JOIN verter v
                                    ON checks.id = v."Check"
                                    FULL JOIN p2p p
                                    ON checks.id = p."Check"
                          WHERE v.state IN ('Failure', NULL)
                             OR p.state = 'Failure') AS c;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task4('res');
FETCH ALL FROM "res";
CLOSE res;
END;

------------------------------------ 5 --------------------------------------------
-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
DROP PROCEDURE IF EXISTS part3_task5;
CREATE OR REPLACE PROCEDURE part3_task5(INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR SELECT checkingpeer AS peer,
                        CASE WHEN give IS NULL THEN receive ELSE receive - give END AS pointschange
                   FROM (SELECT checkingpeer, SUM(pointsamount) AS receive
                           FROM transferredpoints
                          GROUP BY checkingpeer) AS d
                            FULL JOIN (SELECT checkedpeer, SUM(pointsamount) AS give
                                         FROM transferredpoints
                                        GROUP BY checkedpeer) AS b
                            ON checkedpeer = checkingpeer
                  ORDER BY pointschange DESC;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task5('res');
FETCH ALL FROM "res";
CLOSE res;
END;

------------------------------------ 6 --------------------------------------------
-- 6) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3
DROP PROCEDURE IF EXISTS part3_task6;
CREATE OR REPLACE PROCEDURE part3_task6(INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR WITH cte AS (SELECT *
                                FROM part3_task1()
                               UNION
                              SELECT peer2, peer1, (-1) * pointsamount
                                FROM part3_task1()
                               ORDER BY pointsamount DESC)
               SELECT peer1 AS peer, SUM(pointsamount) AS pointschange
                 FROM cte
                GROUP BY peer1
                ORDER BY pointschange DESC;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task6('res');
FETCH ALL FROM "res";
CLOSE res;
END;

------------------------------------ 7 --------------------------------------------
-- 7) Определить самое часто проверяемое задание за каждый день
DROP PROCEDURE IF EXISTS part3_task7;
CREATE OR REPLACE PROCEDURE part3_task7(INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR WITH cte AS (SELECT task, date, COUNT(date) AS count FROM checks GROUP BY date, task ORDER BY date),
                      cte1 AS (SELECT date, MAX(count) AS max FROM cte GROUP BY date)
               SELECT cte1.date AS day, cte.task AS task
                 FROM cte1
                          JOIN cte
                          ON cte.date = cte1.date AND cte.count = cte1.max;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task7('res');
FETCH ALL FROM "res";
CLOSE res;
END;

------------------------------------ 8 --------------------------------------------
-- 8) Определить длительность последней P2P проверки
DROP PROCEDURE IF EXISTS part3_task8;
CREATE OR REPLACE PROCEDURE part3_task8(INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR WITH a AS (SELECT *
                              FROM checks
                                       JOIN p2p p2p2
                                       ON checks.id = p2p2."Check"),
                      b AS (SELECT MAX(date) AS maxdate FROM a WHERE state IN ('Success', 'Failure')),
                      c AS (SELECT MAX(a."time") AS maxtime
                              FROM a,
                                   b
                             WHERE a.date = b.maxdate),
                      d AS (SELECT a."Check" AS acheck
                              FROM a,
                                   c
                             WHERE time = c.maxtime),
                      e AS (SELECT "time" AS starttime
                              FROM p2p,
                                   d
                             WHERE "Check" = acheck
                               AND state = 'Start')
               SELECT (maxtime - starttime)::time AS "check duration"
                 FROM c,
                      e;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task8('res');
FETCH ALL FROM "res";
CLOSE res;
END;

------------------------------------ 9 --------------------------------------------
-- 9) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
DROP PROCEDURE IF EXISTS part3_task9;
CREATE OR REPLACE PROCEDURE part3_task9(block varchar, INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR SELECT peer, MAX(date) AS date
                   FROM (SELECT peer, task, date
                           FROM checks
                                    FULL JOIN verter v
                                    ON checks.id = v."Check"
                                    FULL JOIN p2p p
                                    ON checks.id = p."Check"
                          WHERE (v.state = 'Success' OR v.state IS NULL)
                            AND p.state = 'Success'
                            AND task LIKE '%' || block || '%') AS p
                  GROUP BY peer
                 HAVING COUNT(task) = (SELECT COUNT(title) FROM tasks WHERE title LIKE '%' || block || '%');
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task9('CPP', 'res');
FETCH ALL FROM "res";
CLOSE res;
END;

-- 10) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
-- Определять нужно исходя из рекомендаций друзей пира, т.е. нужно найти пира,
-- проверяться у которого рекомендует наибольшее число друзей.
-- Формат вывода: ник пира, ник найденного проверяющего

DROP PROCEDURE IF EXISTS p3t10(INOUT res refcursor);
CREATE PROCEDURE p3t10(INOUT res refcursor)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN res FOR SELECT tmp.nickname AS peer, tmp.recommendedpeer AS recommendedpeer
                   FROM (SELECT p.nickname, r.recommendedpeer, COUNT(r.recommendedpeer) AS num
                           FROM peers AS p
                                    JOIN friends f
                                    ON p.nickname = f.peer1 OR p.nickname = f.peer2
                                    JOIN recommendations r
                                    ON (f.peer2 = r.peer AND f.peer2 != p.nickname) OR
                                       (f.peer1 = r.peer AND f.peer1 != p.nickname)
                          WHERE p.nickname != r.recommendedpeer
                          GROUP BY p.nickname, r.recommendedpeer) AS tmp
                  WHERE tmp.num = (SELECT MAX(tmp2.num)
                                     FROM (SELECT COUNT(r.recommendedpeer) AS num
                                             FROM peers AS p
                                                      JOIN friends f
                                                      ON p.nickname = f.peer1 OR p.nickname = f.peer2
                                                      JOIN recommendations r
                                                      ON (f.peer2 = r.peer AND f.peer2 != p.nickname) OR
                                                         (f.peer1 = r.peer AND f.peer1 != p.nickname)
                                            WHERE p.nickname != r.recommendedpeer
                                              AND tmp.nickname = p.nickname
                                            GROUP BY p.nickname, r.recommendedpeer) AS tmp2);
END;
$$;

BEGIN;
CALL p3t10('cursor');
FETCH ALL IN "cursor";
COMMIT;
END;

-- 11) Определить процент пиров, которые:
-- Приступили только к блоку 1
-- Приступили только к блоку 2
-- Приступили к обоим
-- Не приступили ни к одному
--
-- Пир считается приступившим к блоку, если он проходил
-- хоть одну проверку любого задания из этого блока (по таблице Checks)
-- Параметры процедуры: название блока 1, например SQL, название блока 2, например A.
-- Формат вывода: процент приступивших только к первому блоку, процент приступивших
-- только ко второму блоку, процент приступивших к обоим, процент не приступивших ни к одному

DROP PROCEDURE IF EXISTS p3t11(INOUT res refcursor, block1 varchar, block2 varchar);
CREATE PROCEDURE p3t11(INOUT res refcursor, block1 varchar, block2 varchar)
    LANGUAGE plpgsql AS
$$
DECLARE
    totalpeers         numeric := (SELECT COUNT(*)
                                     FROM peers);
    startedbothblocks  numeric;
    startedblock1      numeric;
    startedblock2      numeric;
    didntstartanyblock numeric;
BEGIN
    startedbothblocks = (SELECT COUNT(tmp.peer)
                           FROM (SELECT peer
                                   FROM checks
                                  WHERE task SIMILAR TO block1 || '[0-9]' || '%'
                              INTERSECT
                                 SELECT peer
                                   FROM checks
                                  WHERE task SIMILAR TO block2 || '[0-9]' || '%') AS tmp);

    startedblock1 = ROUND(((SELECT COUNT(peer)
                              FROM (SELECT DISTINCT peer
                                      FROM checks
                                     WHERE task SIMILAR TO block1 || '[0-9]' || '%') AS tmp) - startedbothblocks) *
                          100 / totalpeers, 2);

    startedblock2 = ROUND(((SELECT COUNT(peer)
                              FROM (SELECT DISTINCT peer
                                      FROM checks
                                     WHERE task SIMILAR TO block2 || '[0-9]' || '%') AS tmp) - startedbothblocks) *
                          100 / totalpeers, 2);

    didntstartanyblock = ROUND((SELECT COUNT(*)
                                  FROM (SELECT *
                                          FROM checks
                                                   FULL JOIN peers p
                                                   ON checks.peer = p.nickname
                                         WHERE checks.peer IS NULL) AS tmp) * 100 / totalpeers, 2);

    startedbothblocks = ROUND(startedbothblocks * 100 / totalpeers, 2);
    OPEN res FOR SELECT startedblock1, startedblock2, startedbothblocks, didntstartanyblock;
END;
$$;

BEGIN;
CALL p3t11('cursor', 'C', 'CPP');
FETCH ALL IN "cursor";
COMMIT;
END;

-- 12) Определить N пиров с наибольшим числом друзей
-- Параметры процедуры: количество пиров N.
-- Результат вывести отсортированным по кол-ву друзей.
-- Формат вывода: ник пира, количество друзей

DROP PROCEDURE IF EXISTS p3t12(INOUT res refcursor, n bigint);
CREATE PROCEDURE p3t12(INOUT res refcursor, n bigint)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN res FOR SELECT DISTINCT peer1 AS peer, COUNT(peer1) AS friendscount
                   FROM (SELECT f1.peer1, f1.peer2
                           FROM friends f1
                          UNION ALL
                         SELECT f2.peer2, f2.peer1
                           FROM friends f2) AS tmp
                  GROUP BY peer
                  ORDER BY 2 DESC
                  LIMIT n;
END;
$$;

BEGIN;
CALL p3t12('cursor', 3);
FETCH ALL IN "cursor";
COMMIT;
END;

-- 13) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
-- Также определите процент пиров, которые хоть раз проваливали проверку в свой день рождения.
-- Формат вывода: процент успехов в день рождения, процент неуспехов в день рождения

DROP PROCEDURE IF EXISTS p3t13(INOUT res refcursor);
CREATE PROCEDURE p3t13(INOUT res refcursor)
    LANGUAGE plpgsql AS
$$
DECLARE
    totalchecks        numeric := (SELECT COUNT(*)
                                     FROM peers p
                                              JOIN checks c
                                              ON p.nickname = c.peer
                                              JOIN p2p
                                              ON c.id = p2p."Check"
                                    WHERE EXTRACT(DAY FROM p.birthday) = EXTRACT(DAY FROM c.date)
                                      AND EXTRACT(MONTH FROM p.birthday) = EXTRACT(MONTH FROM c.date)
                                      AND state = 'Start');
    successfulchecks   numeric := ROUND((SELECT COUNT(*)
                                           FROM peers p
                                                    JOIN checks c
                                                    ON p.nickname = c.peer
                                                    JOIN p2p
                                                    ON c.id = p2p."Check"
                                          WHERE EXTRACT(DAY FROM p.birthday) = EXTRACT(DAY FROM c.date)
                                            AND EXTRACT(MONTH FROM p.birthday) = EXTRACT(MONTH FROM c.date)
                                            AND state = 'Success') * 100 / totalchecks, 2);
    unsuccessfulchecks numeric := ROUND((SELECT COUNT(*)
                                           FROM peers p
                                                    JOIN checks c
                                                    ON p.nickname = c.peer
                                                    JOIN p2p
                                                    ON c.id = p2p."Check"
                                          WHERE EXTRACT(DAY FROM p.birthday) = EXTRACT(DAY FROM c.date)
                                            AND EXTRACT(MONTH FROM p.birthday) = EXTRACT(MONTH FROM c.date)
                                            AND state = 'Failure') * 100 / totalchecks, 2);
BEGIN
    OPEN res FOR SELECT successfulchecks, unsuccessfulchecks;
END;
$$;

BEGIN;
CALL p3t13('cursor');
FETCH ALL IN "cursor";
COMMIT;
END;

-- 14) Определить кол-во XP, полученное в сумме каждым пиром
-- Если одна задача выполнена несколько раз, полученное за нее кол-во XP равно максимальному за эту задачу.
-- Результат вывести отсортированным по кол-ву XP.
-- Формат вывода: ник пира, количество XP

DROP PROCEDURE IF EXISTS p3t14(INOUT res refcursor);
CREATE PROCEDURE p3t14(INOUT res refcursor)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN res FOR SELECT DISTINCT p.nickname AS peer, SUM(t.maxxp) AS xp
                   FROM peers p
                            JOIN checks c
                            ON p.nickname = c.peer
                            JOIN p2p
                            ON c.id = p2p."Check" AND p2p.state = 'Success'
                            JOIN tasks t
                            ON c.task = t.title
                            LEFT JOIN verter v
                            ON c.id = v."Check" AND (v.state = 'Success' OR v.state IS NULL)
                  GROUP BY p.nickname
                  ORDER BY xp DESC;
END;
$$;

BEGIN;
CALL p3t14('cursor');
FETCH ALL IN "cursor";
COMMIT;
END;

-- 15) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
-- Параметры процедуры: названия заданий 1, 2 и 3.
-- Формат вывода: список пиров

DROP PROCEDURE IF EXISTS p3t15(INOUT res refcursor, first varchar, second varchar, third varchar);
CREATE PROCEDURE p3t15(INOUT res refcursor, first varchar, second varchar, third varchar)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN res FOR SELECT c.peer
                   FROM checks c
                            JOIN p2p
                            ON c.id = p2p."Check" AND p2p.state = 'Success' AND c.task = first
              INTERSECT
                 SELECT c.peer
                   FROM checks c
                            JOIN p2p
                            ON c.id = p2p."Check" AND p2p.state = 'Success' AND c.task = second
              INTERSECT
                 SELECT c.peer
                   FROM checks c
                            FULL JOIN p2p
                            ON c.id = p2p."Check" AND p2p.state IS NULL AND c.task = third;
END;
$$;

BEGIN;
CALL p3t15('cursor', 'C2_SimpleBashUtils', 'C3_s21_string+', 'CPP1_s21_matrix+');
FETCH ALL IN "cursor";
COMMIT;
END;

-- 16) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
-- То есть сколько задач нужно выполнить, исходя из условий входа, чтобы получить доступ к текущей.
-- Формат вывода: название задачи, количество предшествующих

DROP PROCEDURE IF EXISTS p3t16(INOUT res refcursor);
CREATE PROCEDURE p3t16(INOUT res refcursor)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN res FOR WITH RECURSIVE previous_task(title, parenttask, count) AS (SELECT title, parenttask, 0
                                                                              FROM tasks
                                                                             WHERE parenttask IS NULL
                                                                             UNION ALL
                                                                            SELECT t.title, t.parenttask, count + 1
                                                                              FROM previous_task pt,
                                                                                   tasks t
                                                                             WHERE pt.title = t.parenttask)
               SELECT title AS task, count AS prevcount
                 FROM previous_task;
END;
$$;

BEGIN;
CALL p3t16('cursor');
FETCH ALL IN "cursor";
COMMIT;
END;

-- 17) Найти "удачные" для проверок дни. День считается "удачным",
-- если в нем есть хотя бы N идущих подряд успешных проверки
-- Параметры процедуры: количество идущих подряд успешных проверок N.
-- Временем проверки считать время начала P2P этапа.
-- Под идущими подряд успешными проверками подразумеваются успешные проверки, между которыми нет неуспешных.
-- При этом кол-во опыта за каждую из этих проверок должно быть не меньше 80% от максимального.
-- Формат вывода: список дней

DROP PROCEDURE IF EXISTS p3t17(INOUT res refcursor, n bigint);
CREATE PROCEDURE p3t17(INOUT res refcursor, n bigint)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN res FOR SELECT DISTINCT date AS lucky_days
                   FROM (SELECT date, state, ROW_NUMBER() OVER (PARTITION BY date, x ORDER BY date, state) AS k
                           FROM (SELECT date, state, time, xpamount, maxxp,
                                        COUNT(CASE WHEN state != 'Success' THEN 1 END)
                                        OVER (PARTITION BY date ORDER BY date, time) AS x
                                   FROM checks
                                            JOIN p2p
                                            ON checks.id = p2p."Check"
                                            FULL JOIN xp
                                            ON checks.id = xp."Check"
                                            FULL JOIN tasks t
                                            ON checks.task = t.title
                                  WHERE p2p.state != 'Start') AS tmp1
                          WHERE xpamount * 100 / maxxp >= 80) AS tmp2
                  WHERE state != 'Failure'
                    AND k >= n;
END;
$$;

BEGIN;
CALL p3t17('cursor', 3);
FETCH ALL IN "cursor";
COMMIT;
END;

-- 18) Определить пира с наибольшим числом выполненных заданий
-- Формат вывода: ник пира, число выполненных заданий

DROP PROCEDURE IF EXISTS p3t18(INOUT res refcursor);
CREATE PROCEDURE p3t18(INOUT res refcursor)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN res FOR SELECT peer, COUNT(peer) AS number
                   FROM checks
                            JOIN xp x
                            ON checks.id = x."Check"
                  GROUP BY peer
                  ORDER BY number DESC
                  LIMIT 1;
END;
$$;

BEGIN;
CALL p3t18('cursor');
FETCH ALL IN "cursor";
COMMIT;
END;

-- 19) Определить пира с наибольшим количеством XP
-- Формат вывода: ник пира, количество XP

DROP PROCEDURE IF EXISTS p3t19(INOUT res refcursor);
CREATE PROCEDURE p3t19(INOUT res refcursor)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN res FOR SELECT peer, SUM(xpamount) AS xp
                   FROM checks
                            JOIN xp x
                            ON checks.id = x."Check"
                  GROUP BY peer
                  ORDER BY xp DESC
                  LIMIT 1;
END;
$$;

BEGIN;
CALL p3t19('cursor');
FETCH ALL IN "cursor";
COMMIT;
END;

------------------------------------ 20 --------------------------------------------
-- 20) Определить пира, который провел сегодня в кампусе больше всего времени
DROP PROCEDURE IF EXISTS part3_task20;
CREATE OR REPLACE PROCEDURE part3_task20(INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR SELECT peer
                   FROM (WITH cte AS (SELECT peer, time AS in_time, state, ROW_NUMBER() OVER () AS id
                                        FROM timetracking
                                       WHERE date = CURRENT_DATE
                                         AND state = 1
                                       GROUP BY peer, time, state),
                              cte1 AS (SELECT peer, time AS out_time, state, ROW_NUMBER() OVER () AS id
                                         FROM timetracking
                                        WHERE date = CURRENT_DATE
                                          AND state = 2
                                        GROUP BY peer, time, state)
                       SELECT cte.peer AS peer, SUM(out_time - in_time)::time AS time
                         FROM cte1
                                  JOIN cte
                                  ON cte1.id = cte.id
                        GROUP BY cte.peer) AS a
                  ORDER BY time DESC
                  LIMIT 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task20('res');
FETCH ALL FROM "res";
CLOSE res;
END;

------------------------------------ 21 --------------------------------------------
-- 21) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
DROP PROCEDURE IF EXISTS part3_task21;
CREATE OR REPLACE PROCEDURE part3_task21(this_time time, count bigint, INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR SELECT peer
                   FROM (SELECT peer, date
                           FROM timetracking
                          WHERE time < this_time AND state = 1
                          GROUP BY peer, date) AS a
                  GROUP BY peer
                 HAVING COUNT(date) >= count
                  ORDER BY peer;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task21('22:00:00', 2, 'res');
FETCH ALL FROM "res";
CLOSE res;
END;

------------------------------------ 22 --------------------------------------------
-- 22) Определить пиров, выходивших за последние N дней из кампуса больше M раз
DROP PROCEDURE IF EXISTS part3_task22;
CREATE OR REPLACE PROCEDURE part3_task22(n_days integer, m_times integer, INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR SELECT peer
                   FROM (SELECT peer, date, COUNT(*) AS count_
                           FROM timetracking
                          WHERE state = 2
                            AND date >= (NOW()::date - n_days)
                          GROUP BY peer, date) AS a
                  GROUP BY peer
                 HAVING SUM(count_) > m_times
                  ORDER BY peer;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task22(5, 1, 'res');
FETCH ALL FROM "res";
CLOSE res;
END;

------------------------------------ 23 --------------------------------------------
-- 23) Определить пира, который пришел сегодня последним
DROP PROCEDURE IF EXISTS part3_task23;
CREATE OR REPLACE PROCEDURE part3_task23(INOUT res refcursor) AS
$$
BEGIN
    OPEN res FOR SELECT peer
                   FROM (SELECT peer, MIN(time) AS time
                           FROM (SELECT peer, time FROM timetracking WHERE state = 1 AND date = CURRENT_DATE) AS a
                          GROUP BY peer) AS b
                  ORDER BY b.time DESC
                  LIMIT 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task23('res');
FETCH ALL FROM "res";
CLOSE res;
END;

------------------------------------ 24 --------------------------------------------
-- 24) Определить пиров, которые выходили вчера из кампуса больше чем на N минут
DROP PROCEDURE IF EXISTS part3_task24;
CREATE OR REPLACE PROCEDURE part3_task24(INOUT res refcursor, n_minutes bigint) AS
$$
BEGIN
    OPEN res FOR WITH table_in AS (SELECT ROW_NUMBER() OVER () AS id, peer, time AS in_time, state
                                     FROM timetracking
                                    WHERE date = CURRENT_DATE - 1
                                      AND state = 1
                                    GROUP BY peer, time, state
                                   OFFSET 1 ROWS),
                      table_out AS (SELECT ROW_NUMBER() OVER () AS id, peer, time AS out_time, state
                                      FROM timetracking
                                     WHERE date = CURRENT_DATE - 1
                                       AND state = 2
                                     GROUP BY peer, time, state
                                     ORDER BY id DESC
                                    OFFSET 1 ROWS)

               SELECT tmp.peer
                 FROM (SELECT table_in.peer, SUM(in_time - out_time)::time AS sum
                         FROM table_in
                                  JOIN table_out
                                  ON table_in.id - 1 = table_out.id AND table_in.peer = table_out.peer
                        GROUP BY table_in.peer) AS tmp
                WHERE EXTRACT(HOURS FROM sum) * 60 * 60 + EXTRACT(MINUTES FROM sum) * 60 + EXTRACT(SECONDS FROM sum) >
                      n_minutes * 60;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL part3_task24('res', 100);
FETCH ALL FROM "res";
CLOSE res;
END;

-- 25) Определить для каждого месяца процент ранних входов
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус
-- за всё время (будем называть это общим числом входов).
-- Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, приходили в кампус
-- раньше 12:00 за всё время (будем называть это числом ранних входов).
-- Для каждого месяца посчитать процент ранних входов в кампус относительно общего числа входов.
-- Формат вывода: месяц, процент ранних входов

DROP PROCEDURE IF EXISTS p3t25(INOUT res refcursor);
CREATE PROCEDURE p3t25(INOUT res refcursor)
    LANGUAGE plpgsql AS
$$
BEGIN
    OPEN res FOR WITH months AS (SELECT ROW_NUMBER() OVER () AS num, TO_CHAR(gs, 'Month') AS month
                                   FROM (SELECT generate_series AS gs
                                           FROM GENERATE_SERIES('2022-01-01', '2022-12-31', INTERVAL '1 month')) AS s)

               SELECT month,
                      COALESCE((SELECT COUNT(*) * 100 / NULLIF((SELECT COUNT(*)
                                                                  FROM peers p1
                                                                           JOIN timetracking t1
                                                                           ON p1.nickname = t1.peer
                                                                 WHERE EXTRACT(MONTH FROM p1.birthday) = EXTRACT(MONTH FROM t1.date)
                                                                   AND t1.state = 1
                                                                   AND num = EXTRACT(MONTH FROM t1.date)), 0)
                                  FROM peers p
                                           JOIN timetracking t
                                           ON p.nickname = t.peer
                                 WHERE EXTRACT(MONTH FROM p.birthday) = EXTRACT(MONTH FROM t.date)
                                   AND num = EXTRACT(MONTH FROM t.date)
                                   AND t.state = 1
                                   AND EXTRACT(HOURS FROM t.time) < 12), 0) AS earlyentries
                 FROM months;
END;
$$;

BEGIN;
CALL p3t25('cursor');
FETCH ALL IN "cursor";
COMMIT;
END;