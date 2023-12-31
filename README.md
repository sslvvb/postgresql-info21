# Postgresql-info21

Анализ и статистика данных.

## Logical view of database model

![SQL2](./images/SQL2.png)

#### Таблица Peers

- Ник пира
- День рождения

#### Таблица Tasks

- Название задания
- Название задания, являющегося условием входа
- Максимальное количество XP

Чтобы получить доступ к заданию, нужно выполнить задание, являющееся его условием входа.
Для упрощения будем считать, что у каждого задания всего одно условие входа.
В таблице должно быть одно задание, у которого нет условия входа (т.е. поле ParentTask равно null).

#### Статус проверки

Создать тип перечисления для статуса проверки, содержащий следующие значения:
- Start - начало проверки
- Success - успешное окончание проверки
- Failure - неудачное окончание проверки

#### Таблица P2P

- ID
- ID проверки
- Ник проверяющего пира
- [Статус P2P проверки](#статус-проверки)
- Время

Каждая P2P проверка состоит из 2-х записей в таблице: первая имеет статус начало, вторая - успех или неуспех. \
В таблице не может быть больше одной незавершенной P2P проверки, относящейся к конкретному заданию, пиру и проверяющему. \
Каждая P2P проверка (т.е. обе записи, из которых она состоит) ссылается на проверку в таблице Checks, к которой она относится.


#### Таблица Verter

- ID
- ID проверки
- [Статус проверки Verter'ом](#статус-проверки)
- Время 

Каждая проверка Verter'ом состоит из 2-х записей в таблице: первая имеет статус начало, вторая - успех или неуспех. \
Каждая проверка Verter'ом (т.е. обе записи, из которых она состоит) ссылается на проверку в таблице Checks, к которой она относится. \
Проверка Verter'ом может ссылаться только на те проверки в таблице Checks, которые уже включают в себя успешную P2P проверку.

#### Таблица Checks

- ID 
- Ник пира
- Название задания
- Дата проверки

Описывает проверку задания в целом. Проверка обязательно включает в себя **один** этап P2P и, возможно, этап Verter.
Для упрощения будем считать, что пир ту пир и автотесты, относящиеся к одной проверке, всегда происходят в один день.

Проверка считается успешной, если соответствующий P2P этап успешен, а этап Verter успешен, либо отсутствует.
Проверка считается неуспешной, хоть один из этапов неуспешен.
То есть проверки, в которых ещё не завершился этап P2P, или этап P2P успешен, но ещё не завершился этап Verter, не относятся ни к успешным, ни к неуспешным.

#### Таблица TransferredPoints

- ID
- Ник проверяющего пира
- Ник проверяемого пира
- Количество переданных пир поинтов за всё время (только от проверяемого к проверяющему)

При каждой P2P проверке проверяемый пир передаёт один пир поинт проверяющему.
Эта таблица содержит все пары проверяемый-проверяющий и кол-во переданных пир поинтов, то есть, 
другими словами, количество P2P проверок указанного проверяемого пира, данным проверяющим.

#### Таблица Friends

- ID
- Ник первого пира
- Ник второго пира 

Дружба взаимная, т.е. первый пир является другом второго, а второй -- другом первого.

#### Таблица Recommendations

- ID
- Ник пира
- Ник пира, к которому рекомендуют идти на проверку

Каждому может понравиться, как проходила P2P проверка у того или иного пира. 
Пир, указанный в поле Peer, рекомендует проходить P2P проверку у пира из поля RecommendedPeer. 
Каждый пир может рекомендовать как ни одного, так и сразу несколько проверяющих.

#### Таблица XP

- ID
- ID проверки
- Количество полученного XP

За каждую успешную проверку пир, выполнивший задание, получает какое-то количество XP, отображаемое в этой таблице. 
Количество XP не может превышать максимальное доступное для проверяемой задачи. 
Первое поле этой таблицы может ссылаться только на успешные проверки.

#### Таблица TimeTracking

- ID
- Ник пира
- Дата
- Время
- Состояние (1 - пришел, 2 - вышел)

Данная таблица содержит информация о посещениях пирами кампуса. 
Когда пир входит в кампус, в таблицу добавляется запись с состоянием 1, когда покидает - с состоянием 2. 

## Part 1. Создание базы данных

Cкрипт *part1.sql* создает базу данных и все таблицы, описанные выше.

Добавлены процедуры, позволяющие импортировать и экспортировать данные для каждой таблицы из файла/в файл с расширением *.csv*. \
В качестве параметра каждой процедуры указывается разделитель *csv* файла.

## Part 2. Изменение данных

Скрипт *part2.sql* состоит из тестовых запросов/вызовов для изменения данных.

## Part 3. Получение данных

Процедуры и функции для получения данных из БД.

## Part 4. Метаданные

Создание отдельной базу данных, в которой созданы таблицы, функции, процедуры и триггеры, необходимые для тестирования процедур работы с метаданными.
