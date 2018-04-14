import * as assert from 'assert';

import Database from '..';
import { Record } from '../src/database';

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
  db: Database, n = 1000, value: (i: number) => Record
) {
  try {
    await db.drop();
  } catch (e) { }

  try {
    await db.create();
  } catch (e) { }

  const size = Math.min(100_000, n);
  const objects = Array(size);

  console.time(`insert ${n} objects`);
  for (let i = 0; i < n; i++) {
    objects[i % size] = value(i);
    if ((i + 1) % size == 0)
      await db.insert(objects);
  }
  if (n % size != 0)
    await db.insert(objects.slice(0, n % size));
  console.timeEnd(`insert ${n} objects`);
}

async function testFind(
  db: Database, field: string, n = 20,
  value: (i: number) => any, count: (i: number) => number
) {
  for (let i = 0; i < n; i++) {
    const val = value(i);
    console.time(`find ${field}=${val}`);
    const objects = await db.find(field, val);
    console.timeEnd(`find ${field}=${val}`);
    console.log(`${objects.length} results`);
    assert.equal(objects.length, count(val));
    for (const obj of objects)
      assert.equal(obj[field], val);
  }
}

async function main() {
  const n = Number(process.argv[2]) || 10_000;
  const fields = ['name', 'age'];
  const db = new Database(`${__dirname}/data/data-insert-${n}.json`, fields);

  const { array: names, count: namesCount } =
    fillArray(n, _ => Math.random().toString(36));
  const { array: ages, count: agesCount } =
    fillArray(n, _ => Math.round(Math.random() * 100));

  await testInserts(db, n, i => ({ name: names[i], age: ages[i] }));
  await testFind(
    db, 'name', 20, _ => names[getRandomInt(0, n - 1)], val => namesCount[val]
  );
  await testFind(
    db, 'age', 20, _ => ages[getRandomInt(0, n - 1)], val => agesCount[val]
  );
}

main();
