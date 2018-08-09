import File from './file';
import { readJSONSync } from './utils';

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
      readJSONSync(this.file.readSync(position));

    if (!alreadyOpen)
      await this.file.close();

    return { value: value as T, start, length };
  }

  *getAllSync() {
    const alreadyOpen = this.file.isOpen;
    if (!alreadyOpen)
      this.file.openSync();

    const stream = this.file.readSync(0);

    function* start(i: number, char: string) {
      yield [i, char] as [number, string];
      yield* stream;
    }

    try {
      for (const [i, char] of stream) {
        if (char != '{')
          continue;
        yield [i, readJSONSync(start(i, char)).value];
      }
    } finally {
      if (!alreadyOpen)
        this.file.closeSync();
    }
  }

  getSync(position: number) {
    const { value, start, length } =
      readJSONSync(this.file.readSync(position));
    return { value: value as T, start, length };
  }

  async remove(position: number) {
    const { start, length } = readJSONSync(this.file.readSync(position), false);
    let last = false;
    for (const [_, char] of this.file.readSync(start + length)) {
      if (char == ' ' || char == '\n')
        continue;
      else {
        if (char == ']')
          last = true;
        break;
      }
    }
    await this.file.clear(start, length + Number(!last));
  }

  async insert(data: T, position?: number) {
    const alreadyOpen = this.file.isOpen;

    const dataString = this.stringify(data);

    try {
      if (!alreadyOpen)
        await this.file.open();
    } catch (e) {
      if (e.code != 'ENOENT')
        throw e;
      await this.file.write(
        0, `[\n${' '.repeat(this.indent)}${dataString}\n]\n`
      );
      return {
        start: 2 + this.indent,
        length: dataString.length,
        raw: dataString
      };
    }

    if (position != null) {
      let start = position;
      if (position > 0) {
        for (const [i, char] of this.file.readSync(position - 1, true)) {
          if (char == ' ' || char == '\n')
            start = i;
          else
            break;
        }
      }
      start += this.indent + 1;

      let end = position;
      let last = false;
      for (const [i, char] of this.file.readSync(position)) {
        if (char == ' ' || char == '\n')
          end = i;
        else {
          if (char == ']')
            last = true;
          break;
        }
      }
      end -= last ? 1 : this.indent + 1;

      const length = end - start + 1;

      if (dataString.length > length)
        throw new Error('Not enough space to insert');

      await this.file.write(
        start, `${dataString}${last ? '' : ','}`
      );

      if (!alreadyOpen)
        await this.file.close();

      return { start, length: dataString.length, raw: dataString };
    } else {
      const result = await this.appendRaw(dataString, position);
      if (!alreadyOpen)
        await this.file.close();
      return result;
    }
  }

  async append(data: T, position?: number) {
    const dataString = this.stringify(data);
    return await this.appendRaw(dataString, position);
  }

  async appendRaw(dataString: string, position?: number) {
    if (!dataString)
      throw new Error('Cannot append empty string');

    let isFirst = false;

    if (!position) {
      for (const [i, char] of this.file.readSync(-1, true)) {
        if (char == ' ' || char == '\n')
          continue;
        if (!position)
          position = i;
        else {
          if (char == '[')
            isFirst = true;
          break;
        }
      }
    }

    await this.file.write(
      position! - 1,
      `${isFirst ? '' : ','}\n${' '.repeat(this.indent)}${dataString}\n]\n`,
    );

    return {
      start: position! + Number(!isFirst) + this.indent,
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

  async getObjectStart(position: number) {
    let depth = 1;
    let prevChar: string | undefined;
    let inString = false;
    let pos: number | undefined;

    for (const [i, char] of this.file.readSync(position, true)) {
      // Ignore space
      if (char == ' ' || char == '\n')
        continue;
      // Ignore strings
      const isStringQuote = char == '"' && prevChar != '\\';
      prevChar = char;
      if (isStringQuote)
        inString = !inString;
      if (inString)
        continue;

      if (char == '}')
        ++depth;
      else if (char == '{')
        --depth;

      if (char == '{' && !depth) {
        pos = i;
        break;
      }
    }

    return pos;
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

  joinForAppend(dataStrings: string[]) {
    const indent = ' '.repeat(this.indent);
    return dataStrings.join(`,\n${indent}`);
  }
}

export default JSONStore;
