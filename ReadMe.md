# Transaction isolation levels

## A training demo project to demonstrate anomalies (problems), or lack thereof, in parallel transaction execution with different levels of transaction isolation.

## The project is written for instructional purposes.

## The following anomalies (problems) are possible during parallel execution of transactions:
- Lost update (LU),
- Dirty read (DR),
- Non-repeatable read (NRR), 
- Phantom reads (PhR).

## Transaction isolation level options (and possible anomalies):
- Seriliazable,
- Repeated read (PhR),
- Read commited (PhR, NRR, LU),
- Read uncommited (PhR, NRR, DR, LU).

## There are other and anomalies and levels of isolation that will not be realized.

## Resources:

[Уровень изолированности транзакций (Википедия)]

[Understanding isolation levels]

<!-- Links -->
[Уровень изолированности транзакций (Википедия)]: <https://ru.wikipedia.org/wiki/%D0%A3%D1%80%D0%BE%D0%B2%D0%B5%D0%BD%D1%8C_%D0%B8%D0%B7%D0%BE%D0%BB%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D0%BE%D1%81%D1%82%D0%B8_%D1%82%D1%80%D0%B0%D0%BD%D0%B7%D0%B0%D0%BA%D1%86%D0%B8%D0%B9>
[Understanding isolation levels]: <https://learn.microsoft.com/en-us/sql/connect/jdbc/understanding-isolation-levels?view=sql-server-ver17&redirectedfrom=MSDN>
