import JSONStore from './json-store';
import { z85DecodeAsUInt32, z85EncodeAsUInt32 } from './utils';


class Index {
  protected store: JSONStore<SerializedIndexEntry>;
  protected maxHeight = 32;

  constructor(filename: string, private dbStore: JSONStore) {
    this.store = new JSONStore(filename, 0);
  }

  async create() {
    await this.store.create();
  }

  async drop() {
    await this.store.destroy();
  }

  async find(field: string, value: any) {
    const entries = await this.findEntries(field, value);
    return entries.map(entry => entry.node.value);
  }

  async insert(objectFields: ObjectField | ObjectField[]) {
    if (!Array.isArray(objectFields))
      objectFields = [objectFields];

    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

    const cache: IndexCache = {};
    const dbCache: DBCache = {};

    for (const objectField of objectFields) {
      const head = await this.getHead(objectField.field, cache);
      await this.indexObjectField(objectField, head, cache, dbCache);
    }

    if (!alreadyOpen)
      await this.store.close();
  }

  protected async indexObjectField(
    objectField: ObjectField, head: IndexEntry,
    cache?: IndexCache, dbCache?: DBCache
  ) {
    const { field, value, position } = objectField;

    cache = cache || {};
    dbCache = dbCache || {};

    dbCache[position] = value;

    let height = head.node.levels.filter(p => p != 0).length;

    let level = 0;
    for (; Math.round(Math.random()) == 1 && level < this.maxHeight; level++) {
      if (level > height) {
        height = level;
        break;
      }
    }

    const entry = await this.insertEntry(new IndexEntry(
      field, new SkipListNode(position, Array(level + 1).fill(0))
    ), cache);

    const entries = new Set<IndexEntry>();

    let current = head;
    for (let i = height; i >= 0; i--) {
      while (current.node.next(i)) {
        const next = await this.getEntry(current.node.next(i), cache);

        const nextValue = dbCache[next.node.value] ||
          await next.value(this.dbStore);
        dbCache[next.node.value] = nextValue;

        if (nextValue > value)
          break;

        current = next;
      }

      if (i > level)
        continue;

      entry.node.levels[i] = current.node.next(i);
      entries.add(entry);

      current.node.levels[i] = entry.position;
      entries.add(current);
    }

    for (const entry of entries.values())
      await this.updateEntry(entry);
  }

  protected async getHead(field: string, cache?: IndexCache) {
    let head;
    try {
      head = await this.getRootEntry(cache);
    } catch (e) {
      head = await this.insertRootEntry(cache);
    }

    while (head.field != field && head.node.value)
      head = await this.getEntry(head.node.value, cache);

    if (head.field != field) {
      const prevHead = head;
      head = await this.insertEntry(new IndexEntry(
        field, new SkipListNode(0, Array(this.maxHeight).fill(0))
      ), cache);
      prevHead.node.value = head.position;
      await this.updateEntry(prevHead);
    }

    return head;
  }

  protected async getRootEntry(cache?: IndexCache) {
    if (cache && 0 in cache)
      return cache[0];
    const { start, value } = await this.store.get(1);
    const entry = new IndexEntry(value);
    entry.position = start;
    if (cache)
      cache[0] = entry;
    return entry;
  }

  protected async insertRootEntry(cache?: IndexCache) {
    const entry = await this.insertEntry(
      new IndexEntry("", new SkipListNode(0, []))
    );
    if (cache)
      cache[0] = entry;
    return entry;
  }

  protected async insertEntry(entry: IndexEntry, cache?: IndexCache) {
    const { start } = await this.store.insert(entry.serialized());
    entry.position = start;
    if (cache)
      cache[entry.position] = entry;
    return entry;
  }

  protected async getEntry(position: number, cache?: IndexCache) {
    if (cache && position in cache)
      return cache[position];
    const { start, value } = await this.store.get(position);
    const entry = new IndexEntry(value);
    entry.position = start;
    if (cache)
      cache[entry.position] = entry;
    return entry;
  }

  protected async updateEntry(entry: IndexEntry) {
    // offset 4 = 1 brace + 2 quotes + 1 colon
    await this.store.set(
      entry.position + 4 + entry.field.length, entry.node.encoded()
    );
  }

  protected async findEntries(field: string, value: any) {
    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

    const cache: { [postion: number]: IndexEntry } = {};
    const dbCache: { [postion: number]: any } = {};

    const head = await this.getHead(field, cache);
    const height = head.node.levels.filter(p => p != 0).length;

    const entries: IndexEntry[] = [];

    let current = head;
    for (let i = height; i >= 0; i--) {
      while (current.node.next(i)) {
        const next = await this.getEntry(current.node.next(i), cache);

        const nextValue = dbCache[next.node.value] ||
          await next.value(this.dbStore);
        dbCache[next.node.value] = nextValue;

        if (nextValue == value) {
          // Fall to last level to get duplicates
          if (i > 0) {
            i = 1;
            break;
          }
          entries.push(next);
        }
        else if (nextValue > value)
          break;

        current = next;
      }
    }

    if (!alreadyOpen)
      await this.store.close();

    return entries;
  }
}

export class SkipListNode {
  public value: number;
  public levels: number[];

  constructor(encodedNode: string);
  constructor(value: number, levels: number[]);
  constructor(obj: any, levels?: number[]) {
    if (typeof obj == 'string') {
      const encodedNode = obj;
      const parts = encodedNode.split(';');
      const [encodedValue, encodedLevels] = parts;
      this.value = z85DecodeAsUInt32(encodedValue);
      this.levels = encodedLevels.split(',').map(
        s => z85DecodeAsUInt32(s)
      );
    } else {
      const value: number | undefined = obj;
      if (value == null || !levels)
        throw new Error('value and levels are required');
      this.value = value;
      this.levels = levels;
    }
  }

  next(level: number) {
    return this.levels[level];
  }

  encoded() {
    const encodedValue = z85EncodeAsUInt32(this.value);
    const encodedLevels = this.levels.map(z85EncodeAsUInt32).join(',');
    return `${encodedValue};${encodedLevels}`;
  }
}

export class IndexEntry {
  field: string;
  node: SkipListNode;
  position: number = 0;

  constructor(serializedEntry: SerializedIndexEntry);
  constructor(field: string, node: SkipListNode);
  constructor(obj: any, node?: SkipListNode) {
    if (typeof obj == 'string') {
      const field = obj;
      if (!node)
        throw new Error('node is required');
      this.field = field;
      this.node = node;
    } else {
      const serializedEntry = obj as SerializedIndexEntry;
      this.field = Object.keys(serializedEntry)[0];
      const encodedNode = serializedEntry[this.field];
      this.node = new SkipListNode(encodedNode);
    }
  }

  async value(store: Store<any>) {
    return (await store.get(this.node.value)).value;
  }

  serialized(): SerializedIndexEntry {
    return {
      [this.field]: this.node.encoded()
    };
  }
}

export interface SerializedIndexEntry {
  [field: string]: string;
}

export interface ObjectField {
  field: string;
  value: any;
  position: number;
}

export interface IndexCache {
  [postion: number]: IndexEntry;
}

export interface DBCache {
  [postion: number]: any;
}

export default Index;
