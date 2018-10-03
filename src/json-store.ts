import File from './file';
import Store from './store';
import { Char, readJSON } from './utils';

class JSONStore<T> implements Store<T> {
  protected file: File;

  constructor(filename: string, protected indent = 2) {
    this.file = new File(filename);
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

  async create(objects: T[] = []) {
    const i = ' '.repeat(this.indent);
    const content = objects.length ?
      `\n${i}${objects.map(this.stringify.bind(this)).join(`,\n${i}`)}` : '';
    await this.file.open('wx');
    await this.file.write(0, `[${content}\n]\n`);
    await this.file.close();
  }

  async destroy() {
    await this.file.delete();
  }

  async lock(pos = 0, options = { exclusive: false }) {
    await this.file.lock(pos, options);
  }

  async unlock(pos = 0) {
    await this.file.unlock(pos);
  }

  async get(position: number) {
    const { value, start, length } =
      (await readJSON(this.file.read(position)).next()).value;
    return { value: value as T, start, length };
  }

  async *getAll() {
    // Allow line-delimited JSON
    const start = Number(
      (await this.file.read(0).next()).value[1] == Char.LeftBracket
    );

    const stream = readJSON(
      this.file.read(start, false, Buffer.alloc(1 << 16))
    );

    let res;
    while (!(res = await stream.next()).done) {
      const result = res.value;
      yield [result.start, result.value];
    }
  }

  async getAppendPosition() {
    let first = false;
    let done = false;

    let position: number | undefined;

    for await (const chars of this.file.read(-1, true)) {
      for (let i = 0; i < chars.length; i += 2) {
        const codePoint = chars[i + 1];
        if (codePoint == Char.Space || codePoint == Char.Newline)
          continue;
        if (!position) {
          if (codePoint != Char.RightBracket) {
            done = true;
            break;
          }
          position = chars[i] - 1;
        } else {
          if (codePoint == Char.LeftBracket)
            first = true;
          done = true;
          break;
        }
      }
      if (done)
        break;
    }

    if (!position)
      throw new Error('Invalid JSON file');

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
    const valueString = JSON.stringify(value);
    await this.file.write(position, valueString);
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
