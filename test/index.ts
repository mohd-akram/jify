import * as assert from 'assert';
import * as fs from 'fs';
import * as util from 'util';

import Database, { Record } from '../src/database';
import { predicate as p } from '../src/query';
import { IndexField } from '../src/index';
import * as utils from '../src/utils';

const logger = utils.logger('test');

/* Helpers */

function getFilename(filename: string) {
  return `${__dirname}/data/${filename}`;
}

function getField(object: Record, field: string) {
  let value: any = object;
  for (const f of field.split('.'))
    value = value[f];
  return value;
}

function getRandomInt(min: number, max: number) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function fillArray(n: number, value: (i: number) => any) {
  const array = [];
  const count: { [value: number]: number } = {};
  for (let i = 0; i < n; i++) {
    const val = value(i);
    array.push(val);
    count[val] = (count[val] || 0) + 1;
  }
  return { array, count };
}

function compareObjects(a: object, b: object) {
  if (a == b)
    return 0;
  const keys =
    Array.from(new Set(Object.keys(a).concat(Object.keys(b)))).sort();
  for (const key of keys)
    if ((a as any)[key] < (b as any)[key])
      return -1;
    else if ((a as any)[key] > (b as any)[key])
      return 1;
  return 0;
}

function sortObjectArray<T extends object>(arr: T[]) {
  return arr.sort(compareObjects);
}

async function testInserts(
  db: Database, fields: (string | IndexField)[],
  n = 1000, size = 100_000, value: (i: number) => Record
) {
  try {
    await db.drop();
  } catch (e) {
    if (e.code != 'ENOENT')
      throw e;
  }

  await db.create(fields);

  size = Math.min(size, n);
  const objects = Array(size);

  logger.time(`insert ${n} objects`);
  for (let i = 0; i < n; i++) {
    objects[i % size] = value(i);
    if ((i + 1) % size == 0)
      await db.insert(objects);
  }
  if (n % size != 0)
    await db.insert(objects.slice(0, n % size));
  logger.timeEnd(`insert ${n} objects`);
}

async function testFind(
  db: Database, field: string, n = 20,
  value: (i: number) => any, count: (i: number) => number
) {
  for (let i = 0; i < n; i++) {
    const val = value(i);
    logger.time(`find ${field}=${val}`);
    const objects = await db.find({ [field]: val }).toArray();
    logger.timeEnd(`find ${field}=${val}`);
    logger.log(`${objects.length} results`);
    assert.equal(objects.length, count(val));
    for (const obj of objects)
      assert.equal(getField(obj, field), val);
  }
}

/* Tests */

async function testInsertAndFind(n = 10_000, size = 100_000, count = 20) {
  count = Math.min(n, count);

  const fields = [
    'id', 'person.age', { name: 'created', type: 'date-time' }
  ];
  const db = new Database(getFilename(`data-insert-${n}.json`));

  const { array: ids, count: idsCount } =
    fillArray(n, _ =>
      Math.random().toString(36) + '😋'.repeat(Math.random() * 10)
    );
  const { array: ages, count: agesCount } =
    fillArray(n, _ => Math.round(Math.random() * 100));
  const { array: dates, count: datesCount } =
    fillArray(n, _ =>
      new Date(+(new Date()) - Math.floor(Math.random() * 1e10)).toISOString()
    );

  const checkFind = async () => {
    await testFind(
      db, 'id', count, _ => ids[getRandomInt(0, n - 1)], val => idsCount[val]
    );
    await testFind(
      db, 'person.age', count, _ => ages[getRandomInt(0, n - 1)],
      val => agesCount[val]
    );
    await testFind(
      db, 'created', count, _ => dates[getRandomInt(0, n - 1)],
      val => datesCount[val]
    );
  };

  await testInserts(db, fields, n, size, i => ({
    id: ids[i], person: { age: ages[i] }, created: dates[i]
  }));
  await checkFind();
  await (db as any)._index.drop();
  await db.index(...fields);
  await checkFind();
}

async function testQueries() {
  const db = new Database(getFilename('people.json'));

  try {
    await db.drop();
  } catch (e) {
    if (e.code != 'ENOENT')
      throw e;
  }
  await db.create();

  await db.insert({ name: 'John', age: 42 });
  await db.insert({ name: 'John', age: 43 });
  await db.insert({ name: 'John', age: 17 });
  await db.insert({ name: 'John', age: 18 });
  await db.insert({ name: 'John', age: 20 });
  await db.insert({ name: 'John', age: 35 });
  await db.insert({ name: 'John', age: 50 });

  await db.index('name', 'age');

  let results = await db.find({ name: 'John', age: 42 }).toArray();
  assert.deepEqual(
    sortObjectArray(results), sortObjectArray([{ name: 'John', age: 42 }])
  );

  // age < 50
  results = await db.find({ age: p`< ${50}` }).toArray();
  assert.deepEqual(sortObjectArray(results), sortObjectArray([
    { name: 'John', age: 42 },
    { name: 'John', age: 43 },
    { name: 'John', age: 17 },
    { name: 'John', age: 18 },
    { name: 'John', age: 20 },
    { name: 'John', age: 35 }
  ]));

  // 18 <= age < 35
  results = await db.find({ age: p`>= ${18} < ${35}` }).toArray();
  assert.deepEqual(sortObjectArray(results), sortObjectArray([
    { name: 'John', age: 18 },
    { name: 'John', age: 20 }
  ]));

  // age < 18 or age > 35
  results = await db.find({ age: p`< ${18}` }, { age: p`> ${35}` }).toArray();
  assert.deepEqual(sortObjectArray(results), sortObjectArray([
    { name: 'John', age: 42 },
    { name: 'John', age: 43 },
    { name: 'John', age: 17 },
    { name: 'John', age: 50 }
  ]));
}

async function testInvalid() {
  const filename = getFilename('invalid.json');
  try {
    await util.promisify(fs.unlink)(filename);
  } catch (e) {
    if (e.code != 'ENOENT')
      throw e;
  }
  const fd = await util.promisify(fs.open)(filename, 'wx');
  await util.promisify(fs.close)(fd);
  const db = new Database(filename);
  await assert.rejects(db.insert({}));
  await util.promisify(fs.writeFile)(filename, 'invalid');
  await assert.rejects(db.insert({}));
}

async function main() {
  const args = process.argv.slice(2);
  const n = Number(args.shift()) || undefined;
  const size = Number(args.shift()) || undefined;
  const count = Number(args.shift()) || undefined;
  const debug = process.env.DEBUG || '';

  process.env.DEBUG = '';
  await testInsertAndFind(1);
  await testInsertAndFind(200, 20);
  await testQueries();
  await testInvalid();

  process.env.DEBUG = debug;
  await testInsertAndFind(n, size, count);
}

process.once('unhandledRejection', err => { throw err; });

main();
