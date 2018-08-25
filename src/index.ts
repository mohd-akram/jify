import JSONStore from './json-store';
import { Predicate } from './query';
import {
  z85DecodeAsUInt32, z85EncodeAsUInt32,
  z85DecodeAsDouble, z85EncodeAsDouble
} from './utils';

class Index {
  protected store: JSONStore<SerializedIndexEntry>;
  protected maxHeight = 32;

  constructor(public filename: string) {
    this.store = new JSONStore(filename, 0);
  }

  async create() {
    await this.store.create();
    await this.store.open();
    await this.insertRootEntry();
    await this.store.close();
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

  async lastModified() {
    return await this.store.lastModified();
  }

  async find(field: string, predicate: Predicate<SkipListValue>) {
    const entries = await this.findEntries(field, predicate);
    return entries.map(entry => entry.pointer);
  }

  async beginTransaction(field: string) {
    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();
    const head = await this.lockHead(field, true);
    const value = JSON.parse(head.node.value as string);
    if (value.tx)
      throw new IndexError(`Field "${field}" already in transaction`);
    value.tx = 1;
    head.node.value = JSON.stringify(value);
    await this.updateEntry(head);
    await this.unlockHead(head);
    if (!alreadyOpen)
      await this.store.close();
  }

  async endTransaction(field: string) {
    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();
    const head = await this.lockHead(field, true);
    const value = JSON.parse(head.node.value as string);
    if (!value.tx)
      throw new IndexError(`Field "${field}" not in transaction`);
    value.tx = 0;
    head.node.value = JSON.stringify(value);
    await this.updateEntry(head);
    await this.unlockHead(head);
    if (!alreadyOpen)
      await this.store.close();
  }

  async insert(
    objectFields: ObjectField | ObjectField[], cache: IndexCache = new Map()
  ) {
    if (!Array.isArray(objectFields))
      objectFields = [objectFields];

    if (!objectFields.length)
      return;

    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

    const headPosCache: { [field: string]: number } = {};
    const headInfoCache: { [field: string]: IndexFieldInfo } = {};

    const updates = new Set<number>();

    const heads = [];
    let position = -1;
    for (const objectField of objectFields) {
      const cachedHeadPos = headPosCache[objectField.name];
      const cachedHeadInfo = headInfoCache[objectField.name];
      let head: IndexEntry;
      let info: IndexFieldInfo;
      if (cachedHeadPos && cachedHeadInfo) {
        head = cache.get(cachedHeadPos)!;
        info = cachedHeadInfo;
      } else {
        // Lock reads/writes on this field until we're done
        head = await this.lockHead(
          objectField.name, true, cache
        );
        heads.push(head);
        headPosCache[objectField.name] = head.position;
        info = JSON.parse(head.node.value as string);
        headInfoCache[objectField.name] = info;
      }
      if (info.type == 'date-time')
        objectField.value = Date.parse(objectField.value);
      const positions = await this.indexObjectField(
        objectField, head!, position, cache
      );
      for (const position of positions)
        if (position > 0)
          updates.add(position);
      --position;
    }

    // Lock writes completely until we're done to ensure the append position
    // remains correct
    await this.store.lock(0, { exclusive: true });

    const { position: startPosition } = await this.store.getAppendPosition();
    let insertPosition = startPosition;
    const pendingRaw: string[] = [];

    const offset = this.store.joiner.length;

    const insert = (entry: IndexEntry) => {
      const stack = [entry];
      while (stack.length) {
        const entry = stack.pop()!;
        if (entry.position >= 0)
          continue;

        let pending = false;
        for (let i = 0; i < entry.node.levels.length; i++) {
          const pos = entry.node.levels[i];
          if (pos >= 0)
            continue;
          const next = cache.get(pos)!;
          entry.node.levels[i] = next.position;
          if (pos != next.position)
            continue;
          if (!pending)
            stack.push(entry);
          stack.push(next);
          pending = true;
        }

        if (entry.link < 0) {
          const next = cache.get(entry.link)!;
          const link = next.position;
          if (entry.link == link) {
            stack.push(entry);
            stack.push(next);
            pending = true;
          }
          entry.link = link;
        }

        if (!pending) {
          const raw = this.store.stringify(entry.serialized());
          const start = insertPosition + offset;
          const length = raw.length;
          insertPosition = start + length;
          entry.position = start;
          if (cache)
            cache.set(entry.position, entry);
          pendingRaw.push(raw);
        }
      }
    };

    for (let i = -1; i > position; i--)
      insert(cache.get(i)!);

    if (pendingRaw.length)
      await this.store.appendRaw(
        pendingRaw.join(this.store.joiner), startPosition
      );

    for (const pos of updates) {
      const entry = cache.get(pos)!;
      for (let i = 0; i < entry.node.levels.length; i++) {
        const p = entry.node.levels[i];
        if (p < 0)
          entry.node.levels[i] = cache.get(p)!.position;
      }
      if (entry.link < 0)
        entry.link = cache.get(entry.link)!.position;
      await this.updateEntry(entry);
    }

    for (const head of heads)
      await this.unlockHead(head);
    await this.store.unlock();

    if (!alreadyOpen)
      await this.store.close();
  }

  protected async indexObjectField(
    objectField: ObjectField, head: IndexEntry, entryPosition: number,
    cache: IndexCache = new Map(),
  ) {
    const { name, value, position } = objectField;

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
        const next = cache.get(nextNodePos) ||
          await this.getEntry(nextNodePos, cache);
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
      new IndexEntry(name, position, new SkipListNode([])) :
      new IndexEntry(name, position,
        new SkipListNode(Array(level + 1).fill(0), value)
      );

    entry.position = entryPosition; // placeholder position
    cache.set(entry.position, entry);

    if (isDuplicate) {
      entry.link = prev.link;
      prev.link = entry.position;
      return new Set([prev.position]);
    }

    const positions = new Set<number>();

    for (let i = 0; i <= level; i++) {
      const current = updates[updates.length - i - 1];
      entry.node.levels[i] = current.node.next(i);
      current.node.levels[i] = entry.position;
      positions.add(current.position);
    }

    return positions;
  }

  async getFields() {
    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

    let head = await this.getRootEntry();

    const fields: IndexField[] = [];

    while (head.link) {
      head = await this.getEntry(head.link);
      const info = JSON.parse(head.node.value as string);
      fields.push({
        name: head.field,
        ...info
      });
    }

    if (!alreadyOpen)
      await this.store.close();

    return fields;
  }

  async addFields(fields: IndexField[]) {
    if (!fields.length)
      return;

    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

    let head = await this.getRootEntry();

    while (head.link) {
      head = await this.getEntry(head.link);
      fields = fields.filter(f => f.name != head.field);
    }

    let position: number | undefined;
    for (const field of fields) {
      const prevHead = head;
      const info: IndexFieldInfo = { tx: 0 };
      if (field.type == 'date-time')
        info.type = field.type;
      head = new IndexEntry(
        field.name, 0, new SkipListNode(
          Array(this.maxHeight).fill(0), JSON.stringify(info)
        )
      );
      position = await this.insertEntry(head, undefined, position);
      prevHead.link = head.position;
      await this.updateEntry(prevHead);
    }

    if (!alreadyOpen)
      await this.store.close();
  }

  protected async lockHead(
    field: string, exclusive = false, cache?: IndexCache
  ) {
    let head = await this.getRootEntry(cache);

    while (head.field != field && head.link) {
      head = await this.getEntry(head.link, cache);
      if (head.field == field) {
        // Lock and get the entry again since it might have changed
        // just before locking
        await this.store.lock(head.position, { exclusive });
        head = await this.getEntry(head.position, cache);
      }
    }

    if (head.field != field)
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

  protected async insertRootEntry(cache?: IndexCache) {
    const entry = new IndexEntry("", 0, new SkipListNode([]));
    await this.insertEntry(entry);
    if (cache)
      cache.set(0, entry);
    return entry;
  }

  protected async insertEntry(
    entry: IndexEntry, cache?: IndexCache, position?: number
  ) {
    const { start, length } = await this.store.append(
      entry.serialized(), position
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
    // offset 4 = 1 brace + 2 quotes + 1 colon
    await this.store.set(
      entry.position + 4 + entry.field.length, entry.encoded()
    );
  }

  protected async findEntries(field: string, predicate: Predicate<SkipListValue>) {
    const alreadyOpen = this.store.isOpen;
    if (!alreadyOpen)
      await this.store.open();

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

    const entries: IndexEntry[] = [];

    while (current) {
      let entry = current;
      current = current.node.next(0) ?
        await this.getEntry(current.node.next(0), cache) : null;
      const { seek, match } = predicate(entry.node.value);
      if (seek <= 0 && !match)
        continue;
      if (!match)
        break;
      entries.push(entry);
      while (entry.link) {
        const link = await this.getEntry(entry.link, cache);
        entries.push(link);
        entry = link;
      }
    }

    await this.unlockHead(head);

    if (!alreadyOpen)
      await this.store.close();

    return entries;
  }
}

export class IndexError extends Error { }
IndexError.prototype.name = 'IndexError';

export enum SkipListValueType {
  Null,
  Boolean,
  Number,
  String
}

export type SkipListValue = null | boolean | number | string;

export class SkipListNode {
  public levels: number[];
  public value: SkipListValue;

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
      const parts = encodedNode.split(';');
      const [encodedLevels, encodedType] = parts.slice(0, 2);
      const type = z85DecodeAsUInt32(encodedType, true);
      this.value = SkipListNode.decodeValue(type, parts.slice(2).join(';'));
      this.levels = encodedLevels ?
        encodedLevels.split(',').map(l => z85DecodeAsUInt32(l)) : [];
    }
  }

  next(level: number) {
    return this.levels[level];
  }

  encoded() {
    const encodedLevels = this.levels.map(l => z85EncodeAsUInt32(l)).join(',');

    const encodedType = z85EncodeAsUInt32(this.type, true);

    let encodedValue = '';
    if (typeof this.value == 'boolean')
      encodedValue = z85EncodeAsUInt32(Number(this.value), true);
    else if (typeof this.value == 'number')
      encodedValue = z85EncodeAsDouble(this.value, true);
    else if (typeof this.value == 'string')
      encodedValue = this.value;

    return `${encodedLevels};${encodedType};${encodedValue}`;
  }

  protected static decodeValue(type: SkipListValueType, value: string) {
    switch (type) {
      case SkipListValueType.Boolean:
        return Boolean(z85DecodeAsUInt32(value, true));
      case SkipListValueType.Number:
        return z85DecodeAsDouble(value, true);
      case SkipListValueType.String:
        return value;
      default:
        return null;
    }
  }
}

export class IndexEntry {
  position: number = 0;

  field: string;

  pointer: number; // Pointer to object in database
  link: number = 0; // Pointer to next duplicate in index
  node: SkipListNode;

  constructor(serializedEntry: SerializedIndexEntry);
  constructor(field: string, pointer: number, node: SkipListNode);
  constructor(obj: any, pointer?: number, node?: SkipListNode) {
    if (typeof obj == 'string') {
      const field = obj;
      if (pointer == null || !node)
        throw new TypeError('pointer and node are required');
      this.field = field;
      this.pointer = pointer;
      this.node = node;
    } else {
      const serializedEntry = obj as SerializedIndexEntry;
      this.field = Object.keys(serializedEntry)[0];
      const encodedParts = serializedEntry[this.field].split(';');
      const encodedPointer = encodedParts[0];
      const encodedLink = encodedParts[1];
      const encodedNode = encodedParts.slice(2).join(';');
      this.pointer = z85DecodeAsUInt32(encodedPointer);
      this.link = z85DecodeAsUInt32(encodedLink);
      this.node = new SkipListNode(encodedNode);
    }
  }

  encoded() {
    const encodedPointer = z85EncodeAsUInt32(this.pointer);
    const encodedLink = z85EncodeAsUInt32(this.link);
    return `${encodedPointer};${encodedLink};${this.node.encoded()}`;
  }

  serialized(): SerializedIndexEntry {
    return {
      [this.field]: this.encoded()
    };
  }
}

export interface SerializedIndexEntry {
  [field: string]: string;
}

export interface ObjectField {
  name: string;
  value: any;
  position: number;
}

interface IndexFieldInfo {
  type?: string;
  tx?: number;
}

export interface IndexField extends IndexFieldInfo {
  name: string;
}

export type IndexCache = Map<number, IndexEntry>;

export default Index;
