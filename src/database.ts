import * as path from 'path';

import Index, { IndexField, ObjectField } from './index';
import JSONStore from './json-store';
import { Query } from './query';

class Database<T extends Record = Record> {
  protected store: JSONStore<T>;
  protected _index: Index;

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
    await this._index.create();
    await this._index.addFields(this.normalizeIndexFields(indexFields));
  }

  async find(query: Query) {
    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

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
        positions = new Set(fieldPositions);
        continue;
      }

      const intersection = new Set<number>();

      for (const pointer of fieldPositions)
        if (positions.has(pointer))
          intersection.add(pointer);

      positions = intersection;
    }
    positions = positions || new Set();

    const objects: T[] = [];
    for (const pos of positions) {
      const obj = (await this.store.get(pos)).value;
      objects.push(obj);
    }

    if (!alreadyOpen)
      await this.store.close();

    return objects;
  }

  async insert(objects: T | T[]) {
    if (!Array.isArray(objects))
      objects = [objects];

    if (!objects.length)
      return;

    let indexFields: IndexField[] = [];
    try {
      indexFields = await this._index.getFields();
    } catch (e) {
      if (e.code != 'ENOENT')
        throw e;
    }

    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

    const objectFields: ObjectField[] = [];

    let startPosition: number | undefined;
    let position: number | undefined;
    let pendingRaw: string[] = [];

    for (const object of objects) {
      let start: number;
      let length: number;
      if (!position) {
        ({ start, length } = await this.store.append(object));
      } else {
        const raw = this.store.stringify(object);
        start = position + 1 + this.store.indent;
        length = raw.length;
        pendingRaw.push(raw);
      }

      position = start + length + 1;
      if (!startPosition)
        startPosition = position;

      objectFields.push(
        ...this.getObjectFields(object, start, indexFields)
      );
    }

    if (pendingRaw.length)
      this.store.appendRaw(
        this.store.joinForAppend(pendingRaw),
        startPosition
      );

    await this._index.insert(objectFields);

    if (!alreadyOpen)
      await this.store.close();
  }

  async index(...fields: (string | IndexField)[]) {
    let indexOutdated = false;
    let indexExists = true;

    try {
      indexOutdated = await this.isIndexOutdated();
    } catch (e) {
      if (e.code != 'ENOENT')
        throw e;
      indexExists = false;
    }

    let currentIndexFields = new Map<string, IndexField>();

    if (indexExists)
      currentIndexFields = new Map(
        (await this._index.getFields()).map(
          f => [f.name, f] as [string, IndexField]
        )
      );
    if (indexOutdated) {
      await this._index.drop();
      indexExists = false;
    }
    if (!indexExists)
      await this._index.create();

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

    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();
    const objectFields: ObjectField[] = [];
    for (const [pos, object] of this.store.getAllSync()) {
      objectFields.push(
        ...this.getObjectFields(object, pos, indexFields)
      );
    }
    if (!alreadyOpen)
      await this.store.close();

    await this._index.addFields(indexFields);
    await this._index.insert(objectFields);
  }

  async drop() {
    await this.store.destroy();
    try {
      await this._index.drop();
    } catch (e) {
      if (e.code != 'ENOENT')
        throw e;
    }
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
