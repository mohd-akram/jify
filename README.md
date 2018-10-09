jify
====

jify is an experimental library/tool for querying large (GBs) JSON files. It
does this by first indexing the required fields. It can also be used as an
append-only database.

Install
-------

    npm install jify

Usage
-----

```javascript
const { Database, predicate: p } = require('jify');

async function main() {
  const db = new Database('books.json');

  // Create
  await db.create();

  // Insert - Single
  await db.insert({
    title: 'Robinson Crusoe',
    year: 1719,
    author: { name: 'Daniel Defoe' }
  });

  // Insert - Batch
  await db.insert([
    {
      title: 'Great Expectations',
      year: 1861,
      author: { name: 'Charles Dickens' }
    },
    {
      title: 'Oliver Twist',
      year: 1838,
      author: { name: 'Charles Dickens' }
    },
    {
      title: 'Pride and Prejudice',
      year: 1813,
      author: { name: 'Jane Austen' }
    },
    {
      title: 'Nineteen Eighty-Four',
      year: 1949,
      author: { name: 'George Orwell' }
    }
  ]);

  // Index
  await db.index('title', 'year', 'author.name');

  // Query
  console.log('author.name = Charles Dickens, year > 1840');
  const query = { 'author.name': 'Charles Dickens', year: p`> ${1840}` };
  for await (const record of db.find(query))
    console.log(record);

  let records;

  // Range query
  console.log('1800 <= year < 1900');
  records = await db.find({ year: p`>= ${1800} < ${1900}` }).toArray();
  console.log(records);

  // Multiple queries
  console.log('year < 1800 or year > 1900');
  records = await db.find(
    { year: p`< ${1800}` }, { year: p`> ${1900}` }
  ).toArray();
  console.log(records);
}

main();
```

### CLI

```terminal
$ jify index --field title --field author.name --field year books.json
$ jify find --query "author.name=Charles Dickens,year>1840" books.json
$ jify find --query "year>=1800<1900" books.json
$ jify find --query "year<1800" --query "year>1900" books.json
```

Performance
-----------

jify is reasonably fast. It can index about 1M records (~700 MB) per minute and
supports parallel indexing of fields. Inserting (with indexes) has similar
performance. Query time is < 5ms for the first result + (0.1ms find + 0.1ms
fetch) per subsequent result. All tests on a MBP 2016 base model.
