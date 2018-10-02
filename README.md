jify
====

jify is an experimental library for querying large (GBs) JSON files. It does
this by first indexing the required fields. It can also be used as an
append-only database.

Install
-------

    npm install jify

Usage
-----

```javascript
const { Database, predicate: p } = require('jify');

const db = new Database('data.json');

await db.create();

// Insert - Single
await db.insert({ name: 'John', age: 42 });

// Insert - Batch
await db.insert([
  { name: 'John', age: 17 },
  { name: 'Jack', age: 18 },
  { name: 'Jason', age: 20 },
  { name: 'Jim', age: 35 },
  { name: 'Jane', age: 50 }
]);

// Index
await db.index('name', 'age');

// Query
for await (const record of db.find({ name: 'John', age: 42 }))
  console.log(record);

// age < 50
records = await db.find({ age: p`< ${50}` }).toArray();

// 18 <= age < 35
records = await db.find({ age: p`>= ${18} < ${35}` }).toArray();

// age < 18 or age > 35
records = await db.find({ age: p`< ${18}` }, { age: p`> ${35}` }).toArray();
```
