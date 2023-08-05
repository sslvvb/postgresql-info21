---------------------------------------------------------------------------------
------------------- Написать процедуру добавления P2P проверки ------------------
---------------------------------------------------------------------------------

-- процедура добавления п2п проверки
DROP PROCEDURE IF EXISTS pr_add_p2p(VARCHAR, VARCHAR, VARCHAR, check_status, TIME);
CREATE PROCEDURE pr_add_p2p(in_checkedpeer VARCHAR, in_checkingpeer VARCHAR, in_task VARCHAR, in_state check_status,
                            in_time TIME) AS
$$
DECLARE
    check_id INTEGER;
BEGIN
    IF (fn_check_prev_task(in_checkedpeer, in_task) = TRUE) THEN
        IF in_state = 'Start' THEN
               INSERT INTO checks (id, peer, task, date)
               VALUES ((SELECT MAX(id) + 1 FROM checks), in_checkedpeer, in_task, CURRENT_DATE)
            RETURNING id::INTEGER INTO check_id;
        ELSEIF (in_state IN ('Success', 'Failure')) THEN
            SELECT checks.id
              INTO check_id
              FROM checks
                       JOIN p2p
                       ON checks.id = p2p."Check"
             WHERE peer = in_checkedpeer
               AND task = in_task
             GROUP BY checks.id
            HAVING COUNT(p2p."Check") = 1;
        END IF;

        IF check_id IS NOT NULL THEN
            INSERT INTO p2p (id, "Check", checkingpeer, state, time)
            VALUES ((SELECT MAX(id) + 1 FROM p2p), check_id, in_checkingpeer, in_state, in_time);
        END IF;
    END IF;
END
$$ LANGUAGE plpgsql;

-- проверяет выполнение задания
DROP FUNCTION IF EXISTS fn_check_done_task(in_peer VARCHAR, in_task VARCHAR);
CREATE FUNCTION fn_check_done_task(in_peer VARCHAR, in_task VARCHAR) RETURNS boolean AS
$$
DECLARE
    p2p_    check_status;
    verter_ check_status;
BEGIN
    SELECT state
      INTO p2p_
      FROM p2p
               JOIN checks
               ON p2p."Check" = checks.id
     WHERE checks.peer = in_peer
       AND checks.task = in_task
     ORDER BY date DESC, time DESC
     LIMIT 1;
    SELECT state
      INTO verter_
      FROM verter
               JOIN checks
               ON verter."Check" = checks.id
     WHERE checks.peer = in_peer
       AND checks.task = in_task
     ORDER BY date DESC, time DESC
     LIMIT 1;
    IF (p2p_ = 'Success' AND verter_ <> 'Failure') THEN RETURN TRUE; ELSE RETURN FALSE; END IF;
END
$$ LANGUAGE plpgsql;

-- проверяет выполнение предыдущего задания
DROP FUNCTION IF EXISTS fn_check_prev_task(peer VARCHAR, task VARCHAR);
CREATE FUNCTION fn_check_prev_task(peer VARCHAR, task VARCHAR) RETURNS boolean AS
$$
DECLARE
    prev_task VARCHAR;
BEGIN
    SELECT parenttask INTO prev_task FROM tasks WHERE title = task LIMIT 1;
    IF prev_task IS NOT NULL THEN RETURN fn_check_done_task(peer, prev_task); ELSE RETURN TRUE; END IF;
END
$$ LANGUAGE plpgsql;

-- тестовые запросы/вызовы

-- не будет вставлен, так как не выполнено предыдущее задание
CALL pr_add_p2p('Turing', 'Jesus', 'C4_s21math', 'Start', '10:13');
SELECT p2p."Check"
  FROM p2p
           JOIN checks c
           ON p2p."Check" = c.id
 WHERE peer = 'Turing'
   AND checkingpeer = 'Jesus'
   AND state = 'Start'
   AND time = '10:13';

-- вставит и в checks и в p2p
CALL pr_add_p2p('Turing', 'Jesus', 'C3_s21_string+', 'Start', '10:14');
SELECT p2p."Check"
  FROM p2p
           JOIN checks c
           ON p2p."Check" = c.id
 WHERE peer = 'Turing'
   AND checkingpeer = 'Jesus'
   AND state = 'Start'
   AND time = '10:14'
   AND date = CURRENT_DATE;

---------------------------------------------------------------------------------
---------------- Написать процедуру добавления проверки Verter'ом ---------------
---------------------------------------------------------------------------------

-- процедура добавления проверки Verter'ом
DROP PROCEDURE IF EXISTS pr_add_verter(VARCHAR, VARCHAR, VARCHAR, check_status, TIME);
CREATE PROCEDURE pr_add_verter(in_checkedpeer VARCHAR, in_task VARCHAR, in_state check_status, in_time TIME) AS
$$
DECLARE
    check_ INTEGER;
BEGIN
    SELECT checks.id
      INTO check_
      FROM checks
               JOIN p2p p
               ON checks.id = p."Check"
     WHERE task = in_task
       AND state = 'Success'
       AND peer = in_checkedpeer
     ORDER BY time DESC
     LIMIT 1;
    IF (check_ IS NOT NULL) THEN
        INSERT INTO verter (id, "Check", state, time)
        VALUES ((SELECT MAX(id) + 1 FROM verter), check_, in_state, in_time);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- тестовые запросы/вызовы

-- не будет вставлен, так как не p2p проверка небыла успешно завершена
CALL pr_add_verter('Turing', 'C3_s21_string+', 'Start', '13:14');
SELECT verter."Check"
  FROM verter
           JOIN checks c
           ON verter."Check" = c.id
 WHERE peer = 'Turing'
   AND state = 'Start'
   AND time = '13:14'
   AND task = 'C3_s21_string+';

-- добавим verter проверку
CALL pr_add_p2p('Turing', 'Iayako', 'C3_s21_string+', 'Success', '10:17');
CALL pr_add_verter('Turing', 'C3_s21_string+', 'Start', '13:14');
SELECT verter."Check"
  FROM verter
           JOIN checks c
           ON verter."Check" = c.id
 WHERE peer = 'Turing'
   AND state = 'Start'
   AND time = '13:14';

---------------------------------------------------------------------------------
------------------ Написать триггер для P2P / TransferredPoints -----------------
---------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS pr_after_start_in_p2p();
CREATE FUNCTION pr_after_start_in_p2p() RETURNS trigger AS
$$
DECLARE
    checkedpeer_   VARCHAR;
    transfered_id_ bigint;
BEGIN
    IF (new.state = 'Start') THEN
        SELECT peer
          INTO checkedpeer_
          FROM p2p
                   JOIN checks c
                   ON c.id = p2p."Check"
         WHERE p2p."Check" = new."Check"
         LIMIT 1;

        SELECT id
          INTO transfered_id_
          FROM transferredpoints
         WHERE checkingpeer = new.checkingpeer
           AND checkedpeer = checkedpeer_
         LIMIT 1;

        IF (transfered_id_ IS NULL) THEN
            INSERT INTO transferredpoints (id, checkingpeer, checkedpeer, pointsamount)
            VALUES ((SELECT MAX(id) + 1 FROM transferredpoints), new.checkingpeer, checkedpeer_, 1);
        ELSE
            UPDATE transferredpoints
               SET pointsamount = pointsamount + 1
             WHERE checkingpeer = new.checkingpeer
               AND checkedpeer = checkedpeer_;
        END IF;
        RETURN new;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- триггер
DROP TRIGGER IF EXISTS tr_after_start_in_p2p ON p2p;
CREATE TRIGGER tr_after_start_in_p2p
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE pr_after_start_in_p2p();

-- добавит новую запись в таблицу transferredpoints
CALL pr_add_p2p('Turing', 'Jesus', 'C4_s21math', 'Start', '17:16');
SELECT *
  FROM transferredpoints
 WHERE checkingpeer = 'Jesus'
   AND checkedpeer = 'Turing';

-- сделает +1 в таблице transferredpoints
CALL pr_add_p2p('Turing', 'Jesus', 'C4_s21math', 'Failure', '19:14');
CALL pr_add_p2p('Turing', 'Jesus', 'C4_s21math', 'Start', '17:16');
SELECT *
  FROM transferredpoints
 WHERE checkingpeer = 'Jesus'
   AND checkedpeer = 'Turing';

---------------------------------------------------------------------------------
---------------------------- Написать триггер для XP ----------------------------
---------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS pr_before_insert_in_xp();
CREATE FUNCTION pr_before_insert_in_xp() RETURNS trigger AS
$$
DECLARE
    max_xp_for_task_   boolean;
    success_for_check_ boolean;
BEGIN
    IF (new.xpamount <= (SELECT t.maxxp
                           FROM checks c
                                    JOIN tasks t
                                    ON c.task = t.title
                          WHERE c.id = new."Check")) THEN
        max_xp_for_task_ = TRUE;
    ELSE
        max_xp_for_task_ = FALSE;
    END IF;

    IF (EXISTS(SELECT *
                 FROM checks c
                          JOIN verter v
                          ON c.id = v."Check"
                          JOIN p2p p
                          ON c.id = p."Check"
                WHERE c.id = new."Check"
                  AND p.state = 'Success'
                  AND v.state = 'Success')) THEN
        success_for_check_ = TRUE;
    ELSE
        success_for_check_ = FALSE;
    END IF;

    IF (max_xp_for_task_ = TRUE AND success_for_check_ = TRUE) THEN RETURN new; END IF;
    RETURN old;
END;
$$ LANGUAGE plpgsql;

-- триггер
DROP TRIGGER IF EXISTS tr_before_insert_in_xp ON xp;
CREATE TRIGGER tr_before_insert_in_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE PROCEDURE pr_before_insert_in_xp();

-- инсерт не будет выполнен, хп выше положенного
INSERT INTO xp (id, "Check", xpamount)
VALUES ((SELECT MAX(id) + 1 FROM xp), 1, 999);
SELECT *
  FROM xp
 WHERE "Check" = 1
   AND xpamount = 999;

-- инсерт не будет выполнен, проверка завершилась с ошибкой
INSERT INTO xp (id, "Check", xpamount)
VALUES ((SELECT MAX(id) + 1 FROM xp), 7, 200);
SELECT *
  FROM xp
 WHERE "Check" = 7
   AND xpamount = 200;

-- инсерт будет выполнен
INSERT INTO xp (id, "Check", xpamount)
VALUES ((SELECT MAX(id) + 1 FROM xp), 1, 200);
SELECT *
  FROM xp
 WHERE "Check" = 1
   AND xpamount = 200;