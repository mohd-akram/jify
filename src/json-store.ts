import File from './file';
import { readJSON } from './utils';

class JSONStore<T extends object = object> implements Store<T> {
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
    try {
      const { value, start, length } =
        await readJSON(this.file.read(position));
      return { value: value as T, start, length };
    } finally {
      if (!alreadyOpen)
        await this.file.close();
    }
  }

  async remove(position: number) {
    const { start, length } = await readJSON(this.file.read(position), false);
    let last = false;
    for (const [_, char] of this.file.read(start + length)) {
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

  async insert(data: T, position?: number, last = false) {
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

    if (position != null && !last) {
      let start = position;
      if (position > 0) {
        for (const [i, char] of this.file.read(position - 1, true)) {
          if (char == ' ' || char == '\n')
            start = i;
          else
            break;
        }
      }
      start += this.indent + 1;

      let end = position;
      let last = false;
      for (const [i, char] of this.file.read(position)) {
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
      let isFirst = false;
      if (!position) {
        for (const [i, char] of this.file.read(-1, true)) {
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
      if (!alreadyOpen)
        await this.file.close();
      return {
        start: position! + Number(!isFirst) + this.indent,
        length: dataString.length,
        raw: dataString
      };
    }
  }

  async set(position: number, value: any) {
    const valueString = JSON.stringify(value);
    await this.file.write(position, valueString);
  }

  async getObjectStart(position: number) {
    let depth = 1;
    let prevChar;
    let inString = false;
    let pos;

    for (const [i, char] of this.file.read(position, true)) {
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

  protected stringify(data: T, comma = false) {
    const str = this.indent ? JSON.stringify([data], null, this.indent).slice(
      2 + this.indent, -2
    ) : JSON.stringify(data);
    return comma ? str + ',' : str;
  }
}

export default JSONStore;
