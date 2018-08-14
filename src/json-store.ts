import File from './file';
import Store from './store';
import { readJSON } from './utils';

class JSONStore<T extends object = object> implements Store<T> {
  protected file: File;

  constructor(filename: string, protected indent = 2) {
    this.file = new File(filename, Buffer.alloc(1 << 12));
  }

  get isOpen() {
    return this.file.isOpen;
  }

  async open() {
    await this.file.open();
  }

  async close() {
    await this.file.close();
  }

  async create() {
    await this.file.open('wx');
    await this.file.write(0, '[\n]\n');
    await this.file.close();
  }

  async destroy() {
    await this.file.delete();
  }

  async get(position: number) {
    const alreadyOpen = this.file.isOpen;
    if (!alreadyOpen)
      await this.file.open();

    const { value, start, length } =
      readJSON(this.file.readSync(position));

    if (!alreadyOpen)
      await this.file.close();

    return { value: value as T, start, length };
  }

  *getAllSync() {
    const alreadyOpen = this.file.isOpen;
    if (!alreadyOpen)
      this.file.openSync();

    const stream = this.file.readSync(0);

    function* chain<T>(a: T, b: IterableIterator<T>) {
      yield a;
      yield* b;
    }

    try {
      let chars: [number, number][] = [];
      let last = -1;

      while (true) {
        if (last >= chars.length - 1) {
          const res = stream.next();
          if (res.done)
            break;
          chars = res.value;
          last = -1;
        }

        for (++last; last < chars.length; last++) {
          const [, charCode] = chars[last];
          if (charCode == 123) // left brace
            break;
        }

        if (last == chars.length)
          continue;

        const result = readJSON(chain(chars, stream), last);

        yield [result.start, result.value];

        chars = result.chars;
        last = result.index;
      }
    } finally {
      if (!alreadyOpen)
        this.file.closeSync();
    }
  }

  getSync(position: number) {
    const { value, start, length } =
      readJSON(this.file.readSync(position));
    return { value: value as T, start, length };
  }

  async getAppendPosition() {
    let first = false;
    let position = 0;
    for (const chars of this.file.readSync(-1, true)) {
      for (const [i, charCode] of chars) {
        if (charCode == 32 || charCode == 10) // space or newline
          continue;
        if (!position)
          position = i - 1;
        else {
          if (charCode == 91) // left bracket
            first = true;
          break;
        }
      }
    }
    return { position, first };
  }

  async append(data: T, position?: number) {
    const dataString = this.stringify(data);
    return await this.appendRaw(dataString, position);
  }

  async appendRaw(dataString: string, position?: number, first = false) {
    if (!dataString)
      throw new Error('Cannot append empty string');

    if (!position)
      ({ position, first } = await this.getAppendPosition());

    const joiner = first ? this.joiner.slice(1) : this.joiner;

    await this.file.write(
      position!, `${joiner}${dataString}\n]\n`,
    );

    return {
      start: position! + joiner.length,
      length: dataString.length,
      raw: dataString
    };
  }

  async set(position: number, value: any) {
    const alreadyOpen = this.file.isOpen;
    if (!alreadyOpen)
      await this.file.open();
    const valueString = JSON.stringify(value);
    await this.file.write(position, valueString);
    if (!alreadyOpen)
      await this.file.close();
  }

  async lastModified() {
    return (await this.file.stat()).mtime;
  }

  stringify(data: T) {
    const str = this.indent ? JSON.stringify([data], null, this.indent).slice(
      2 + this.indent, -2
    ) : JSON.stringify(data);
    return str;
  }

  get joiner() {
    return `,\n${' '.repeat(this.indent)}`;
  }
}

export default JSONStore;
