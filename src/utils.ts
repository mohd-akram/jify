import { Console } from 'console';
import * as fs from 'fs';
import { Writable } from 'stream';
import * as util from 'util';

import * as z85 from 'z85';

const enum JSONType {
  Unknown, Array, Object, String
}

const enum Char {
  Space = 32,
  Quote = 34,
  Comma = 44,
  Newline = 10,
  Backslash = 92,
  LeftBrace = 123,
  RightBrace = 125,
  LeftBracket = 91,
  RightBracket = 93
}

export async function* readJSON(
  stream: AsyncIterableIterator<[number, number][]>, parse = true
) {
  let charCodes: number[] = [];
  let type = JSONType.Unknown;
  let start = -1;
  let length = 0;

  let depth = 0;
  let inString = false;
  let escaping = false;

  let res: IteratorResult<[number, number][]>;
  while (!(res = await stream.next()).done) {
    for (let index = 0; index < res.value.length; index++) {
      const [i, charCode] = res.value[index];
      if (start == -1) {
        if (
          charCode == Char.Space || charCode == Char.Newline ||
          charCode == Char.Comma
        )
          continue;
        else {
          start = i;
          if (charCode == Char.LeftBrace)
            type = JSONType.Object;
          else if (charCode == Char.Quote)
            type = JSONType.String;
          else if (charCode == Char.LeftBracket)
            type = JSONType.Array;
        }
      }

      ++length;
      if (parse)
        charCodes.push(charCode);

      const isStringQuote = charCode == Char.Quote && !escaping;
      if (escaping)
        escaping = false;
      else if (charCode == Char.Backslash)
        escaping = true;

      if (isStringQuote)
        inString = !inString;

      if (inString && type != JSONType.String)
        continue;

      switch (type) {
        case JSONType.Array:
          if (charCode == Char.LeftBracket)
            ++depth;
          else if (charCode == Char.RightBracket)
            --depth;
          break;
        case JSONType.Object:
          if (charCode == Char.LeftBrace)
            ++depth;
          else if (charCode == Char.RightBrace)
            --depth;
          break;
        case JSONType.String:
          if (isStringQuote)
            depth = Number(!depth);
          break;
        default:
          if (
            charCode == Char.Space || charCode == Char.Newline ||
            charCode == Char.Comma || charCode == Char.RightBrace ||
            charCode == Char.RightBracket
          ) {
            --depth;
            // We only know if a primitive ended on the next character
            // so undo it
            --length;
            if (parse)
              charCodes.pop();
          } else if (!depth)
            ++depth;
      }

      if (!depth) {
        const result: {
          start: number, length: number, value?: any
        } = { start, length };

        if (parse)
          result.value = JSON.parse(
            String.fromCharCode.apply(null, charCodes)
          );

        yield result;

        // Reset
        charCodes = [];
        type = JSONType.Unknown;
        start = -1;
        length = 0;
      }
    }
  }
}

export function findJSONfield(text: string, field: string) {
  let prevChar: string | undefined;
  let inString = false;
  let depth = 0;
  let str = '';

  // TODO - review this code especially depth
  for (let i = 0; i < text.length; i++) {
    const char = text[i];

    if (char == ' ' || char == '\n')
      continue;

    const isStringQuote = char == '"' && prevChar != '\\';

    prevChar = char;

    if (isStringQuote) {
      inString = !inString;
      if (inString)
        str = '';
    }

    if (inString) {
      if (!isStringQuote)
        str += char;
      continue;
    }

    if (char == '}')
      --depth;
    else if (char == '{')
      ++depth;

    if (depth > 1)
      continue;

    // TODO - ensure only one object is searched
    if (char == ':' && str == field)
      return i + 1;
  }

  return;
}

function utf8charcode(buf: Buffer, i: number) {
  const c = buf[i];
  switch (c >> 4) {
    case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
      // 0xxxxxxx
      return c;
    case 12: case 13:
      // 110xxxxx 10xxxxxx
      return ((c & 0x1F) << 6) | (buf[i + 1] & 0x3F);
    case 14:
      // 1110xxxx 10xxxxxx 10xxxxxx
      return (
        ((c & 0x0F) << 12) |
        ((buf[i + 1] & 0x3F) << 6) |
        (buf[i + 2] & 0x3F)
      );
    case 15:
      if (!(c & 0x8))
        // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        return (
          ((c & 0x07) << 18) |
          ((buf[i + 1] & 0x3F) << 12) |
          ((buf[i + 2] & 0x3F) << 6) |
          (buf[i + 3] & 0x3F)
        );
    default:
      throw new Error('Invalid UTF-8');
  }
}

const fstat = util.promisify(fs.fstat);
const fread = util.promisify(fs.read);

export async function* read(
  fd: number, position = 0, reverse = false, buffer?: Buffer
) {
  if (position < 0)
    position += (await fstat(fd)).size;

  buffer = buffer || Buffer.alloc(1 << 12);
  const size = buffer.length;

  let pos = reverse ? position - size + 1 : position;

  const chars: [number, number][] = Array(size);
  for (let i = 0; i < size; i++)
    chars[i] = Array(2) as [number, number];

  let bytesRead = 0;
  let charCount = 0;

  let continuing = null;

  while (true) {
    if (!continuing) {
      charCount = 0;
      const length = Math.min(size, size + pos);

      pos = Math.max(pos, 0);

      ({ bytesRead } = await fread(fd, buffer, 0, length, pos));

      for (let i = 0; i < bytesRead;) {
        let index = reverse ? (bytesRead - i - 1) : i;

        let count = 0;
        if (reverse) {
          count = 1;
          while ((buffer[index] & 0xc0) == 0x80)
            --index, ++count;
        } else {
          for (let b = 7; (buffer[index] >> b) & 1; b--)
            ++count;
          count = count || 1;
        }

        if (index < 0 || index + count > buffer.length)
          throw new Error('Cannot handle this');

        chars[charCount][0] = pos + index;
        chars[charCount][1] = utf8charcode(buffer, index);
        ++charCount;

        i += count;
      }
    }

    continuing = null;

    try {
      if (charCount == size)
        yield chars;
      else
        yield chars.slice(0, charCount);
      // If we reached here, it means we yielded successfully and can update
      // the state for the next iteration.
      continuing = false;
      if (bytesRead < size || (reverse && !pos))
        break;
      pos += reverse ? -size : size;
    } finally {
      if (continuing == null) {
        // If we reached here, then we didn't yield because the generator ended
        // prematurely, so remember to yield in the next iteration.
        continuing = true;
        continue;
      }
    }
  }
}

// Allocating buffers is expensive, so preallocate one
const uint32Buffer = Buffer.alloc(4);
export function z85EncodeAsUInt32(number: number, compact = false) {
  uint32Buffer.writeUInt32BE(number, 0);
  const encoded = z85.encode(uint32Buffer);
  return compact ? encoded.replace(/^0+/, '') : encoded;
}

export function z85DecodeAsUInt32(string: string, compact = false) {
  if (string.length > 5)
    throw new Error('Cannot decode string longer than 5 characters');
  const decoded = z85.decode(compact ? string.padStart(5, '0') : string);
  return decoded.readUInt32BE(0);
}

const doubleBuffer = Buffer.alloc(8);
export function z85EncodeAsDouble(number: number, compact = false) {
  doubleBuffer.writeDoubleBE(number, 0);
  const encoded = z85.encode(doubleBuffer);
  return compact ? encoded.replace(/0+$/, '') : encoded;
}

export function z85DecodeAsDouble(string: string, compact = false) {
  if (string.length > 10)
    throw new Error('Cannot decode string longer than 5 characters');
  const decoded = z85.decode(compact ? string.padEnd(10, '0') : string);
  return decoded.readDoubleBE(0);
}

class Out extends Writable {
  constructor(protected label: string) { super(); }
  _write(chunk: string, _: string, callback: (error?: Error | null) => void) {
    process.stdout.write(`${this.label}: ${chunk}`);
    if (callback)
      callback(null);
  }
}
class Err extends Writable {
  constructor(protected label: string) { super(); }
  _write(chunk: string, _: string, callback: (error?: Error | null) => void) {
    process.stderr.write(`${this.label}: ${chunk}`);
    if (callback)
      callback(null);
  }
}

const dummyConsole = new Console(new Writable);

export function logger(name: string, log?: boolean) {
  if (log == null)
    log = Boolean(process.env.DEBUG);
  return log ? new Console(new Out(name), new Err(name)) : dummyConsole;
}
