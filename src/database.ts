import * as path from 'path';

import Index from './index';
import JSONStore from './json-store';

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

  async find(field: string, value: any) {
    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

    const positions = await this.index.find(field, value);

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

    let position;
    for (const object of objects) {
      // TypeScript needs some help to get the type
      const res: { start: number, length: number, raw: string } =
        await this.store.insert(
          object, position, true
        );
      const { start, length } = res;
      position = start + length + 1;
      for (const { field, key } of this.indexedFields) {
        const value = Database.getField(object, field);
        if (value == undefined)
          continue;
        objectFields.push({ field, value: key(value), position: start });
      }
    }

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
