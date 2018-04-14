import * as path from 'path';

import Index from './index';
import JSONStore from './json-store';

class Database<T extends Record = Record> {
  protected store: JSONStore<T>;
  protected index: Index;

  constructor(filename: string, protected indexedFields: string[] = []) {
    this.store = new JSONStore<T>(filename);
    const dirname = path.dirname(filename);
    const ext = path.extname(filename);
    const basename = path.basename(filename, ext);
    const indexFilename = `${path.join(dirname, basename)}.index${ext}`;
    this.index = new Index(indexFilename);
  }

  async create() {
    await this.store.create();
    await this.index.create(this.indexedFields);
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
      for (const field of this.indexedFields) {
        if (field in object)
          objectFields.push({
            field, value: object[field], position: start
          });
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
}

export interface Record {
  [field: string]: any;
}

export default Database;
