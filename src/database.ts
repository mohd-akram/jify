import * as path from 'path';
import * as child_process from 'child_process';

import Index, { IndexField, IndexFieldInfo, ObjectField } from './index';
import JSONStore from './json-store';
import { Query } from './query';
import { logger } from './utils';

class DatabaseIterableIterator<T> implements AsyncIterableIterator<T> {
  constructor(protected iterator: AsyncIterableIterator<[number, T]>) { }
  async next() {
    const res = (await this.iterator.next()) as IteratorResult<any>;
    if (!res.done)
      res.value = res.value[1];
    return res as IteratorResult<T>;
  }
  async toArray() {
    const array = [];
    for await (const i of this)
      array.push(i);
    return array;
  }
  [Symbol.asyncIterator]() {
    return this;
  }
}

class Database<T extends Record = Record> {
  protected store: JSONStore<T>;
  protected _index: Index;
  protected logger = logger('database');

  constructor(filename: string) {
    this.store = new JSONStore<T>(filename);

    const dirname = path.dirname(filename);
    const ext = path.extname(filename);
    const basename = path.basename(filename, ext);
    const indexFilename = `${path.join(dirname, basename)}.index${ext}`;
    this._index = new Index(indexFilename);
  }

  async create(indexFields: (string | IndexField)[] = []) {
    await this.store.create();
    if (indexFields.length) {
      await this._index.create();
      await this._index.open();
      await this._index.addFields(this.normalizeIndexFields(indexFields));
      await this._index.close();
    }
  }

  async drop() {
    await this.store.destroy();
    try {
      await this._index.drop();
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code != 'ENOENT')
        throw e;
    }
  }

  find(...queries: Query[]) {
    return new DatabaseIterableIterator<T>(async function* (this: Database<T>) {
      let positions: Set<number> | undefined;

      let indexAlreadyOpen = this._index.isOpen;

      if (!indexAlreadyOpen)
        await this._index.open();

      for (const query of queries) {
        const queryPositions = await this.findQuery(query);
        if (!positions) {
          positions = queryPositions;
          continue;
        }
        for (const position of queryPositions)
          positions.add(position);
      }

      if (!indexAlreadyOpen)
        await this._index.close();

      if (!positions)
        return;

      const alreadyOpen = this.store.isOpen;
      if (!alreadyOpen)
        await this.store.open();
      try {
        for (const position of positions) {
          const res = await this.store.get(position);
          yield [res.start, res.value] as [number, T];
        }
      } finally {
        if (!alreadyOpen)
          await this.store.close();
      }
    }.bind(this)());
  }

  protected async findQuery(query: Query) {
    this.logger.time('find');
    let positions: Set<number> | undefined;
    for (const field in query) {
      if (positions && !positions.size)
        break;

      let predicate = query[field];
      if (typeof predicate != 'function') {
        let start = predicate;
        let converted = false;
        predicate = (value: any) => {
          if (predicate.key && !converted) {
            start = predicate.key(start);
            converted = true;
          }
          return {
            seek: value < start ? -1 : value > start ? 1 : 0,
            match: value == start
          };
        };
      }

      const fieldPositions = await this._index.find(field, predicate);

      if (!positions) {
        positions = fieldPositions;
        continue;
      }

      const intersection = new Set<number>();

      for (const position of fieldPositions)
        if (positions.has(position))
          intersection.add(position);

      positions = intersection;
    }
    positions = positions || new Set();
    this.logger.timeEnd('find');

    return positions;
  }

  async insert(objects: T | T[]) {
    if (!Array.isArray(objects))
      objects = [objects];

    if (!objects.length)
      return;

    let indexAlreadyOpen = this._index.isOpen;
    let indexExists = true;

    if (!indexAlreadyOpen) {
      try {
        await this._index.open();
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code != 'ENOENT')
          throw e;
        indexExists = false;
      }
    }

    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();
    await this.store.lock(Number.MAX_SAFE_INTEGER, { exclusive: true });

    try {
      let indexFields: IndexField[] = [];

      if (indexExists) {
        indexFields = await this._index.getFields();
        for (const { name } of indexFields)
          await this._index.beginTransaction(name);
      }

      const objectFieldsMap: { [field: string]: ObjectField[] } = {};

      let { position: startPosition, first } =
        await this.store.getAppendPosition();
      let insertPosition = startPosition - Number(first);
      const pendingRaw: Buffer[] = [];

      let joiner = first ? this.store.joiner.slice(1) : this.store.joiner;
      const offset = this.store.joiner.length;

      this.logger.time('inserts');
      for (const object of objects) {
        const start = insertPosition + offset;

        const raw = Buffer.from(`${joiner}${this.store.stringify(object)}`);

        if (first) {
          joiner = this.store.joiner;
          first = false;
        }

        pendingRaw.push(raw);

        insertPosition += raw.length;

        if (indexExists) {
          for (const o of this.getObjectFields(object, start, indexFields)) {
            const objectFields = objectFieldsMap[o.name];
            if (objectFields)
              objectFields.push(o);
            else
              objectFieldsMap[o.name] = [o];
          }
        }
      }
      this.logger.timeEnd('inserts');

      pendingRaw.push(Buffer.from(this.store.trail));
      await this.store.write(Buffer.concat(pendingRaw), startPosition);

      if (indexExists) {
        this.logger.time('indexing');
        if (indexFields.length) {
          await Promise.all(Object.values(objectFieldsMap).map(
            objectFields => this._index.insert(objectFields))
          );
        }
        this.logger.timeEnd('indexing');
        for (const { name } of indexFields)
          await this._index.endTransaction(name);
      }
    } finally {
      await this.store.unlock(Number.MAX_SAFE_INTEGER);
      if (!alreadyOpen)
        await this.store.close();
      if (indexExists && !indexAlreadyOpen)
        await this._index.close();
    }
  }

  async index(...fields: (string | IndexField)[]) {
    let indexOutdated = false;
    let indexExists = true;
    let indexAlreadyOpen = this._index.isOpen;

    if (!indexAlreadyOpen) {
      try {
        await this._index.open();
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code != 'ENOENT')
          throw e;
        indexExists = false;
      }
    }

    let currentIndexFields = new Map<string, IndexFieldInfo>();

    if (indexExists) {
      indexOutdated = await this.isIndexOutdated();
      currentIndexFields = new Map(
        (await this._index.getFields()).map(
          f => [f.name, f] as [string, IndexFieldInfo]
        )
      );
      for (const field of currentIndexFields.values()) {
        if (field.tx) {
          indexOutdated = true;
          break;
        }
      }
    }
    if (indexOutdated) {
      await this._index.close();
      await this._index.drop();
      indexExists = false;
    }
    if (!indexExists) {
      await this._index.create();
      await this._index.open();
    }

    const newIndexFields = new Map<string, IndexField>();
    for (const field of this.normalizeIndexFields(fields))
      if (!currentIndexFields.has(field.name))
        newIndexFields.set(field.name, field);

    const indexFields = Array.from(
      (indexOutdated ? new Map([...currentIndexFields, ...newIndexFields])
        : newIndexFields).values()
    );

    if (!indexFields.length)
      return;

    await this._index.addFields(indexFields);
    for (const { name } of indexFields)
      await this._index.beginTransaction(name);

    const subprocesses: { [field: string]: child_process.ChildProcess } = {};
    const batches: { [field: string]: ObjectField[] } = {};

    for (const { name } of indexFields) {
      subprocesses[name] = child_process.fork(
        `${__dirname}/indexer`, [this._index.filename]
      );
      subprocesses[name].once('error', err => { throw err; });
      subprocesses[name].once('exit', code => {
        // Can be null
        if (code != 0) {
          delete subprocesses[name];
          for (const subprocess of Object.values(subprocesses))
            subprocess.kill();
          throw new Error('Error in subprocess');
        }
      });
      batches[name] = [];
    }

    await Promise.all(Object.values(subprocesses).map(Database.waitForReady));

    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();
    this.logger.time('read records');
    for await (const [pos, object] of this.store.getAll()) {
      for (const o of this.getObjectFields(object, pos, indexFields)) {
        const batch = batches[o.name];
        batch.push(o);
        if (batch.length == 10_000) {
          batches[o.name] = [];
          subprocesses[o.name].send(batch);
        }
      }
    }
    this.logger.timeEnd('read records');
    if (!alreadyOpen)
      await this.store.close();

    for (const [name, subprocess] of Object.entries(subprocesses)) {
      subprocess.send(batches[name]);
      subprocess.send(null as any);
    }

    await Promise.all(Object.values(subprocesses).map(Database.waitForClose));

    for (const { name } of indexFields)
      await this._index.endTransaction(name);

    if (!indexAlreadyOpen)
      await this._index.close();
  }

  private static async waitForReady(subprocess: child_process.ChildProcess) {
    const timeout = setInterval(() => { }, ~0 >>> 1);
    await new Promise<void>(resolve => {
      subprocess.once('message', message => {
        if (message == 'ready') {
          clearInterval(timeout);
          resolve();
        }
      });
    });
  }

  private static async waitForClose(subprocess: child_process.ChildProcess) {
    const timeout = setInterval(() => { }, ~0 >>> 1);
    await new Promise<void>(resolve => {
      subprocess.once('close', () => {
        clearInterval(timeout);
        resolve();
      });
    });
  }

  protected async isIndexOutdated() {
    const [dbModified, indexModified] = await Promise.all([
      this.store.lastModified(),
      this._index.lastModified()
    ]);
    return indexModified < dbModified;
  }

  protected getObjectFields(
    object: Record, position: number, fields: IndexField[]
  ) {
    const objectFields: ObjectField[] = [];
    for (const { name } of fields) {
      const value = Database.getField(object, name);
      if (value == undefined)
        continue;
      objectFields.push({ name, value, position });
    }
    return objectFields;
  }

  protected normalizeIndexFields(
    indexFields: (string | IndexField)[]
  ): IndexField[] {
    const map = new Map<string, IndexField>();
    for (const f of indexFields) {
      if (typeof f == 'string')
        map.set(f, { name: f });
      else
        map.set(f.name, f);
    }
    return Array.from(map.values());
  }

  protected static getField(object: Record, field: string) {
    let value: any = object;
    for (const f of field.split('.')) {
      if (!value)
        return;
      value = value[f];
    }
    return value;
  }
}

export interface Record {
  [field: string]: any;
}

export default Database;
