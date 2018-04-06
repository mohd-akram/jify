import * as assert from 'assert';

import Database from '..';
import { Record } from '../src/database';

function getRandomInt(min: number, max: number) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function fillArray(n: number, value: (i: number) => any) {
  const array = [];
  for (let i = 0; i < n; i++)
    array.push(value(i));
  return array;
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

  console.time(`insert ${n} objects`);
  let objects = [];
  for (let i = 0; i < n; i++) {
    objects.push(value(i));
    if ((i + 1) % 1000 == 0) {
      await db.insert(objects);
      objects = [];
    }
  }
  if (objects.length)
    await db.insert(objects);
  console.timeEnd(`insert ${n} objects`);
}

async function testFind(
  db: Database, field: string, n = 20, value: (i: number) => any
) {
  for (let i = 0; i < n; i++) {
    const val = value(i);
    console.time(`find ${field}=${val}`);
    const objects = await db.find(field, val);
    console.timeEnd(`find ${field}=${val}`);
    assert.ok(objects.length > 0);
    for (const obj of objects)
      assert.equal(obj[field], val);
  }
}

async function testDuplicates(
  db: Database, field: string, value: number, count: number
) {
  const objects = await db.find(field, value);
  assert.equal(objects.length, count);
}

async function main() {
  const n = Number(process.argv[2]) || 10_000;
  const fields = ['name', 'age'];
  const db = new Database(`${__dirname}/data/data-insert-${n}.json`, fields);

  await testInserts(db, n, i => ({ age: i + 1 }));
  await testFind(db, 'age', 20, _ => getRandomInt(1, n));

  await testInserts(db, n, () => ({ 'age': 4 }));
  await testDuplicates(db, 'age', 4, n);

  const names = fillArray(n, _ => Math.random().toString(36));
  const ages = fillArray(n, _ => Math.round(Math.random() * 100));
  await testInserts(db, n, i => ({ name: names[i], age: ages[i] }));
  await testFind(db, 'name', 20, _ => names[getRandomInt(0, n - 1)]);
  await testFind(db, 'age', 20, _ => ages[getRandomInt(0, n - 1)]);
}

main();
