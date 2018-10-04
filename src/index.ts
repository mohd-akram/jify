import JSONStore from './json-store';
import { Predicate } from './query';
import {
  logger,
  z85DecodeAsUInt, z85EncodeAsUInt,
  z85DecodeAsDouble, z85EncodeAsDouble
} from './utils';

class Index {
  protected store: JSONStore<string>;
  protected maxHeight = 32;
  protected logger = logger('index');

  constructor(public filename: string) {
    this.store = new JSONStore(filename, 0);
  }

  async create() {
    const entry = new IndexEntry(0, new SkipListNode([]));
    await this.store.create([entry.encoded()]);
  }

  async open() {
    await this.store.open();
  }

  async close() {
    await this.store.close();
  }

  async drop() {
    await this.store.destroy();
  }

  get isOpen() {
    return this.store.isOpen;
  }

  async lastModified() {
    return await this.store.lastModified();
  }

  async beginTransaction(field: string) {
    const head = await this.lockHead(field, true);
    const value = JSON.parse(head.node.value as string);
    if (value.tx)
      throw new IndexError(`Field "${field}" already in transaction`);
    value.tx = 1;
    head.node.value = JSON.stringify(value);
    await this.updateEntry(head);
    await this.unlockHead(head);
  }

  async endTransaction(field: string) {
    const head = await this.lockHead(field, true);
    const value = JSON.parse(head.node.value as string);
    if (!value.tx)
      throw new IndexError(`Field "${field}" not in transaction`);
    value.tx = 0;
    head.node.value = JSON.stringify(value);
    await this.updateEntry(head);
    await this.unlockHead(head);
  }

  async insert(
    objectFields: ObjectField | ObjectField[], cache: IndexCache = new Map()
  ) {
    if (!Array.isArray(objectFields))
      objectFields = [objectFields];

    if (!objectFields.length)
      return;

    const fieldName = objectFields[0].name;
    const head = await this.lockHead(fieldName, true, cache);
    const info: IndexFieldInfo = JSON.parse(head.node.value as string);
    const isDateTime = info.type == 'date-time';

    const transform = (o: ObjectField) => {
      if (isDateTime && typeof o.value == 'string')
        o.value = Date.parse(o.value);
    };

    if (objectFields.length == 1) {
      transform(objectFields[0]);
    } else {
      // Sort in descending order to allow a single insert
      objectFields.sort((b, a) => {
        transform(a);
        transform(b);
        return a.value < b.value ? -1 : a.value > b.value ? 1 : 0;
      });
    }

    const inserts: IndexEntry[] = [];
    const updates = new Map<number, IndexEntry>();

    this.logger.time(`traversing index entries - ${fieldName}`);
    for (const objectField of objectFields) {
      const entries = await this.indexObjectField(
        objectField, head!, cache, inserts
      );
      for (const entry of entries)
        if (entry.position > 0)
          updates.set(entry.position, entry);
    }
    this.logger.timeEnd(`traversing index entries - ${fieldName}`);

    // Lock writes completely until we're done to ensure the append position
    // remains correct
    await this.store.lock(0, { exclusive: true });

    const { position: startPosition } = await this.store.getAppendPosition();
    let insertPosition = startPosition;
    const pendingRaw: string[] = [];

    const offset = this.store.joiner.length;

    const process = (entry: IndexEntry) => {
      const position = insertPosition + offset;
      entry.position = position;
      for (let i = 0; i < entry.node.levels.length; i++) {
        const pos = entry.node.levels[i];
        if (pos >= 0)
          continue;
        const next = inserts[-pos - 1];
        entry.node.levels[i] = next.position;
      }
      const raw = this.store.stringify(entry.encoded());
      insertPosition = position + raw.length;
      pendingRaw.push(raw);
    };

    this.logger.time(`inserting index entries - ${fieldName}`);
    for (let i = 0; i < inserts.length; i++) {
      const entry = inserts[i];
      // Insert all the duplicates first so that the main entry can link
      // to the first duplicate
      const value = entry.node.value;
      let prev = null;
      let dupe = null;
      if (entry.node.isDuplicate) {
        process(entry);
        prev = entry;
      }
      while ((dupe = inserts[i + 1]) && dupe.node.value == value) {
        if (prev)
          dupe.link = prev.position;
        process(dupe);
        prev = dupe;
        ++i;
      }
      if (!entry.node.isDuplicate) {
        if (prev)
          entry.link = prev.position;
        process(entry);
      }
    }
    this.logger.timeEnd(`inserting index entries - ${fieldName}`);

    if (pendingRaw.length)
      await this.store.appendRaw(
        pendingRaw.join(this.store.joiner), startPosition
      );

    await this.store.unlock();

    this.logger.time(`updating index entries - ${fieldName}`);
    for (const entry of updates.values()) {
      for (let i = 0; i < entry.node.levels.length; i++) {
        const p = entry.node.levels[i];
        if (p < 0)
          entry.node.levels[i] = inserts[-p - 1].position;
      }
      if (entry.link < 0)
        entry.link = inserts[-entry.link - 1].position;
      await this.updateEntry(entry);
    }
    this.logger.timeEnd(`updating index entries - ${fieldName}`);

    await this.unlockHead(head);
  }

  protected async indexObjectField(
    objectField: ObjectField, head: IndexEntry,
    cache: IndexCache = new Map(), inserts: IndexEntry[]
  ) {
    const { value, position } = objectField;

    let height = head.node.levels.filter(p => p != 0).length;

    const maxLevel = Math.min(height, this.maxHeight - 1);

    let level = 0;
    while (level < maxLevel && Math.round(Math.random()))
      ++level;

    height = Math.max(height, level + 1);

    const updates: IndexEntry[] = [];

    let current = head;

    for (let i = height - 1; i >= 0; i--) {
      let nextNodePos: number;
      while (nextNodePos = current.node.next(i)) {
        // Check cache ourselves to avoid promise overhead
        const next = nextNodePos < 0 ? inserts[-nextNodePos - 1] :
          cache.get(nextNodePos) || await this.getEntry(nextNodePos, cache);
        if (next.node.value! <= value)
          current = next;
        if (next.node.value! >= value)
          break;
      }

      if (i > level)
        continue;

      updates.push(current);
    }

    const prev = updates[updates.length - 1];
    const isDuplicate = prev.node.value == value;

    const entry = isDuplicate ?
      new IndexEntry(position, new SkipListNode([], value)) :
      new IndexEntry(position,
        new SkipListNode(Array(level + 1).fill(0), value)
      );

    inserts.push(entry);
    entry.position = -inserts.length; // placeholder position

    if (isDuplicate) {
      entry.link = prev.link;
      prev.link = entry.position;
      return [prev];
    }

    for (let i = 0; i <= level; i++) {
      const current = updates[updates.length - i - 1];
      entry.node.levels[i] = current.node.next(i);
      current.node.levels[i] = entry.position;
    }

    return updates;
  }

  async getFields() {
    let head = await this.getRootEntry();

    const fields: IndexFieldInfo[] = [];

    while (head.link) {
      head = await this.getEntry(head.link);
      const info: IndexFieldInfo = JSON.parse(head.node.value as string);
      fields.push(info);
    }

    return fields;
  }

  async addFields(fields: IndexField[]) {
    if (!fields.length)
      return;

    let head = await this.getRootEntry();

    while (head.link) {
      head = await this.getEntry(head.link);
      const name =
        (JSON.parse(head.node.value as string) as IndexFieldInfo).name;
      fields = fields.filter(f => f.name != name);
    }

    let position: number | undefined;
    for (const field of fields) {
      const prevHead = head;
      const info: IndexFieldInfo = { name: field.name, tx: 0 };
      if (field.type == 'date-time')
        info.type = field.type;
      head = new IndexEntry(
        0, new SkipListNode(
          Array(this.maxHeight).fill(0), JSON.stringify(info)
        )
      );
      position = await this.insertEntry(head, undefined, position);
      prevHead.link = head.position;
      await this.updateEntry(prevHead);
    }
  }

  protected async lockHead(
    field: string, exclusive = false, cache?: IndexCache
  ) {
    let head = await this.getRootEntry(cache);
    let name = '';

    while (name != field && head.link) {
      head = await this.getEntry(head.link, cache);
      name = (JSON.parse(head.node.value as string) as IndexFieldInfo).name;
      if (name == field) {
        // Lock and get the entry again since it might have changed
        // just before locking
        await this.store.lock(head.position, { exclusive });
        head = await this.getEntry(head.position, cache);
      }
    }

    if (name != field)
      throw new IndexError(`Field "${field}" missing from index`);

    return head;
  }

  protected async unlockHead(head: IndexEntry) {
    await this.store.unlock(head.position);
  }

  protected async getRootEntry(cache?: IndexCache) {
    const cached = cache && cache.get(0);
    if (cached)
      return cached;
    const { start, value } = await this.store.get(1);
    const entry = new IndexEntry(value);
    entry.position = start;
    if (cache)
      cache.set(0, entry);
    return entry;
  }

  protected async insertEntry(
    entry: IndexEntry, cache?: IndexCache, position?: number
  ) {
    const { start, length } = await this.store.append(
      entry.encoded(), position
    );
    entry.position = start;
    if (cache)
      cache.set(entry.position, entry);
    return start + length;
  }

  protected async getEntry(position: number, cache?: IndexCache) {
    const cached = cache && cache.get(position);
    if (cached)
      return cached;
    const { start, value } = await this.store.get(position);
    const entry = new IndexEntry(value);
    entry.position = start;
    if (cache)
      cache.set(entry.position, entry);
    return entry;
  }

  protected async updateEntry(entry: IndexEntry) {
    await this.store.set(entry.position, entry.encoded());
  }

  async find(field: string, predicate: Predicate<SkipListValue>) {
    const cache: IndexCache = new Map();

    const head = await this.lockHead(field, false, cache);
    const height = head.node.levels.filter(p => p != 0).length;

    const info: IndexFieldInfo = JSON.parse(head.node.value as string);

    if (info.tx)
      throw new IndexError(`Field "${field}" in transaction`);

    if (info.type == 'date-time')
      predicate.key = Date.parse as (s: SkipListValue) => number;

    let found = false;

    let current: IndexEntry | null = head;
    for (let i = height - 1; i >= 0; i--) {
      let nextNodePos: number;
      while (nextNodePos = current.node.next(i)) {
        const next = await this.getEntry(nextNodePos, cache);
        const { seek } = predicate(next.node.value);
        if (seek <= 0)
          current = next;
        if (seek == 0)
          found = true;
        if (seek >= 0)
          break;
      }
      if (found)
        break;
    }

    if (current == head)
      current = current.node.next(0) ?
        await this.getEntry(current.node.next(0), cache) : null;

    const pointers = new Set<number>();

    while (current) {
      let entry = current;
      current = current.node.next(0) ?
        await this.getEntry(current.node.next(0), cache) : null;
      const { seek, match } = predicate(entry.node.value);
      if (seek <= 0 && !match)
        continue;
      if (!match)
        break;
      pointers.add(entry.pointer);
      while (entry.link) {
        const link = await this.getEntry(entry.link, cache);
        pointers.add(link.pointer);
        entry = link;
      }
    }

    await this.unlockHead(head);

    return pointers;
  }
}

export class IndexError extends Error { }
IndexError.prototype.name = 'IndexError';

const enum SkipListValueType {
  Null,
  Boolean,
  Number,
  String
}

type SkipListValue = null | boolean | number | string;

class SkipListNode {
  public levels: number[] = [];
  public value: SkipListValue = null;

  get type() {
    return typeof this.value == 'boolean' ? SkipListValueType.Boolean :
      typeof this.value == 'number' ? SkipListValueType.Number :
        typeof this.value == 'string' ? SkipListValueType.String :
          SkipListValueType.Null;
  }

  constructor(encodedNode: string);
  constructor(levels: number[], value?: SkipListValue);
  constructor(obj: any, value?: SkipListValue) {
    if (Array.isArray(obj)) {
      const levels: number[] = obj;
      if (!levels)
        throw new TypeError('levels is required');
      if (typeof value == 'number' && !Number.isFinite(value))
        throw new TypeError('Number value must be finite');
      this.value = value == null ? null : value;
      this.levels = levels;
    } else {
      const encodedNode = obj as string;
      if (!encodedNode)
        return;
      const parts = encodedNode.split(';');
      const [encodedLevels, encodedType] = parts.slice(0, 2);
      const type = z85DecodeAsUInt(encodedType, true);
      this.value = SkipListNode.decodeValue(type, parts.slice(2).join(';'));
      this.levels = encodedLevels ?
        encodedLevels.split(',').map(l => z85DecodeAsUInt(l)) : [];
    }
  }

  get isDuplicate() {
    return this.levels.length == 0;
  }

  next(level: number) {
    return this.levels[level];
  }

  encoded() {
    if (this.isDuplicate)
      return '';

    const encodedLevels = this.levels.map(l => z85EncodeAsUInt(l)).join(',');

    const encodedType = z85EncodeAsUInt(this.type, true);

    let encodedValue = '';
    if (typeof this.value == 'boolean')
      encodedValue = z85EncodeAsUInt(Number(this.value), true);
    else if (typeof this.value == 'number')
      encodedValue = z85EncodeAsDouble(this.value, true);
    else if (typeof this.value == 'string')
      encodedValue = this.value;

    return `${encodedLevels};${encodedType};${encodedValue}`;
  }

  protected static decodeValue(type: SkipListValueType, value: string) {
    switch (type) {
      case SkipListValueType.Boolean:
        return Boolean(z85DecodeAsUInt(value, true));
      case SkipListValueType.Number:
        return z85DecodeAsDouble(value, true);
      case SkipListValueType.String:
        return value;
      default:
        return null;
    }
  }
}

class IndexEntry {
  position: number = 0;

  pointer: number; // Pointer to object in database
  link: number = 0; // Pointer to next duplicate in index
  node: SkipListNode;

  constructor(encodedEntry: string);
  constructor(pointer: number, node: SkipListNode);
  constructor(obj: any, node?: SkipListNode) {
    if (typeof obj == 'string') {
      const encodedEntry = obj as string;
      const encodedParts = encodedEntry.split(';');
      const encodedPointer = encodedParts[0];
      const encodedLink = encodedParts[1];
      const encodedNode = encodedParts.slice(2).join(';');
      this.pointer = z85DecodeAsUInt(encodedPointer);
      this.link = z85DecodeAsUInt(encodedLink);
      this.node = new SkipListNode(encodedNode);
    } else {
      if (obj == null || !node)
        throw new TypeError('pointer and node are required');
      this.pointer = obj;
      this.node = node;
    }
  }

  encoded() {
    const encodedPointer = z85EncodeAsUInt(this.pointer);
    const encodedLink = z85EncodeAsUInt(this.link);
    return `${encodedPointer};${encodedLink};${this.node.encoded()}`;
  }
}

export interface ObjectField {
  name: string;
  value: any;
  position: number;
}

export interface IndexFieldInfo extends IndexField {
  tx?: number;
}

export interface IndexField {
  name: string;
  type?: string;
}

export interface IndexCache {
  get(key: number): IndexEntry | undefined;
  set(key: number, value: IndexEntry): void;
}

export default Index;
