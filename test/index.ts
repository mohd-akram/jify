import * as assert from 'assert';

import Database, { Record } from '../src/database';
import { IndexField } from '../src/index';
import * as utils from '../src/utils';

const logger = utils.logger('test');

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
    const objects = await db.find({ [field]: val });
    logger.timeEnd(`find ${field}=${val}`);
    logger.log(`${objects.length} results`);
    assert.equal(objects.length, count(val));
    for (const obj of objects)
      assert.equal(getField(obj, field), val);
  }
}

async function main() {
  const args = process.argv.slice(2);
  const n = Number(args.shift()) || 10_000;
  const size = Number(args.shift()) || 100_000;
  const count = Math.min(n, 20);

  const fields = [
    'id', 'person.age', { name: 'created', type: 'date-time' }
  ];
  const db = new Database(`${__dirname}/data/data-insert-${n}.json`);

  const { array: ids, count: idsCount } =
    fillArray(n, _ => Math.random().toString(36));
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

process.on('unhandledRejection', err => { throw err; });

main();
