# pgkons

Safely explore your database without risk of side effects. The `pgkons` tool allows one to explore the database, make temporary changes, and see the results. Every operation is handled within a transaction that is rolled back upon exit. 

### Tasks

1. revamp prompui to allows dependency injection of reader/writer
2. add lexer to searchFn to allow for special characters and different actions beyond select/search (like sort/limit/yada yada)
2. Add alterations programmatically
