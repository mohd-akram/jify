import * as path from 'path';

import Index from './index';
import JSONStore from './json-store';
import { Query } from './query';

class Database<T extends Record = Record> {
  protected store: JSONStore<T>;
  protected index: Index;
  protected indexedFields: IndexField[];

  constructor(filename: string, indexedFields: (string | IndexField)[] = []) {
    this.store = new JSONStore<T>(filename);

    const dirname = path.dirname(filename);
    const ext = path.extname(filename);
    const basename = path.basename(filename, ext);
    const indexFilename = `${path.join(dirname, basename)}.index${ext}`;
    this.index = new Index(indexFilename);

    this.indexedFields = indexedFields.map(
      f => typeof f == 'string' ? { field: f, key: (v: any) => v } : f
    );
  }

  async create() {
    await this.store.create();
    await this.index.create(this.indexedFields.map(f => f.field));
  }

  async find(query: Query) {
    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

    let positions: Set<number> | undefined;
    for (const field in query) {
      if (positions && !positions.size)
        break;

      const indexField = this.indexedFields.find(f => f.field == field);
      if (!indexField)
        throw new Error(`Field "${field}" not indexed`);

      let predicate = query[field];
      if (typeof predicate == 'function') {
        predicate.key = indexField.key;
      } else {
        const start = indexField.key(predicate);
        predicate = (value: any) => ({
          seek: value < start ? -1 : value > start ? 1 : 0,
          match: value == start
        });
      }

      const fieldPositions = await this.index.find(field, predicate);

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

    const objects = [];
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

    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

    const objectFields = [];

    let startPosition;
    let position;
    let output: string[] = [];

    for (const object of objects) {
      // TypeScript needs some help to get the type
      let start: number;
      let length: number;
      if (!position) {
        const res: { start: number, length: number, raw: string } =
          await this.store.insert(
            object, position, true
          );
        ({ start, length } = res);
      } else {
        const raw = this.store.stringify(object);
        start = position + 1 + this.store.indent;
        length = raw.length;
        output.push(raw);
      }
      position = start + length + 1;
      if (!startPosition)
        startPosition = position;
      for (const { field, key } of this.indexedFields) {
        const value = Database.getField(object, field);
        if (value == undefined)
          continue;
        objectFields.push({ field, value: key(value), position: start });
      }
    }

    this.store.appendRaw(
      this.store.joinForAppend(output),
      startPosition
    );

    await this.index.insert(objectFields);

    if (!alreadyOpen)
      await this.store.close();
  }

  async drop() {
    await this.store.destroy();
    try {
      await this.index.drop();
    } catch (e) {
      if (e.code != 'ENOENT')
        throw e;
    }
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

export interface IndexField {
  field: string;
  key: (value: any) => any;
}

export default Database;
