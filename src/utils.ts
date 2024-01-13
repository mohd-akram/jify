import { Console } from "console";
import { FileHandle } from "fs/promises";
import { Writable } from "stream";

import * as z85 from "z85";

const enum JSONType {
  Unknown,
  Array,
  Object,
  String,
}

export const enum Char {
  Space = 32,
  Quote = 34,
  Comma = 44,
  Newline = 10,
  Backslash = 92,
  LeftBrace = 123,
  RightBrace = 125,
  LeftBracket = 91,
  RightBracket = 93,
}

export async function* readJSON(stream: AsyncIterator<number[]>, parse = true) {
  let charCodes: number[] = [];
  let type = JSONType.Unknown;
  let start = -1;

  let depth = 0;
  let inString = false;
  let escaping = false;

  let res: IteratorResult<number[]>;
  while (!(res = await stream.next()).done) {
    for (let i = 0; i < res.value.length; i += 2) {
      const codePoint = res.value[i + 1];
      if (start == -1) {
        if (
          codePoint == Char.Space ||
          codePoint == Char.Newline ||
          codePoint == Char.Comma
        )
          continue;
        else {
          start = res.value[i];
          if (codePoint == Char.LeftBrace) type = JSONType.Object;
          else if (codePoint == Char.Quote) type = JSONType.String;
          else if (codePoint == Char.LeftBracket) type = JSONType.Array;
        }
      }

      if (parse) {
        if (codePoint > 0xffff) {
          const code = codePoint - 0x10000;
          charCodes.push(0xd800 | (code >> 10), 0xdc00 | (code & 0x3ff));
        } else charCodes.push(codePoint);
      }

      const isStringQuote = codePoint == Char.Quote && !escaping;
      if (escaping) escaping = false;
      else if (codePoint == Char.Backslash) escaping = true;

      if (isStringQuote) inString = !inString;

      if (inString && type != JSONType.String) continue;

      switch (type) {
        case JSONType.Array:
          if (codePoint == Char.LeftBracket) ++depth;
          else if (codePoint == Char.RightBracket) --depth;
          break;
        case JSONType.Object:
          if (codePoint == Char.LeftBrace) ++depth;
          else if (codePoint == Char.RightBrace) --depth;
          break;
        case JSONType.String:
          if (isStringQuote) depth = Number(!depth);
          break;
        default:
          if (
            codePoint == Char.Space ||
            codePoint == Char.Newline ||
            codePoint == Char.Comma ||
            codePoint == Char.RightBrace ||
            codePoint == Char.RightBracket
          ) {
            --depth;
            if (parse) charCodes.pop();
          } else if (!depth) ++depth;
      }

      if (!depth) {
        const length = res.value[i] - start + Number(type != JSONType.Unknown);

        const result: {
          start: number;
          length: number;
          value?: any;
        } = { start, length };

        if (parse)
          result.value = JSON.parse(String.fromCharCode.apply(null, charCodes));

        yield result;

        // Reset
        charCodes = [];
        type = JSONType.Unknown;
        start = -1;
      }
    }
  }
  return undefined;
}

export function findJSONfield(text: string, field: string) {
  let prevChar: string | undefined;
  let inString = false;
  let depth = 0;
  let str = "";

  // TODO - review this code especially depth
  for (let i = 0; i < text.length; i++) {
    const char = text[i];

    if (char == " " || char == "\n") continue;

    const isStringQuote = char == '"' && prevChar != "\\";

    prevChar = char;

    if (isStringQuote) {
      inString = !inString;
      if (inString) str = "";
    }

    if (inString) {
      if (!isStringQuote) str += char;
      continue;
    }

    if (char == "}") --depth;
    else if (char == "{") ++depth;

    if (depth > 1) continue;

    // TODO - ensure only one object is searched
    if (char == ":" && str == field) return i + 1;
  }

  return;
}

function utf8codepoint(buf: Array<number> | Buffer, i = 0) {
  const c = buf[i];
  switch (c >> 4) {
    case 0:
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
      // 0xxxxxxx
      return c;
    case 12:
    case 13:
      // 110xxxxx 10xxxxxx
      return ((c & 0x1f) << 6) | (buf[i + 1] & 0x3f);
    case 14:
      // 1110xxxx 10xxxxxx 10xxxxxx
      return (
        ((c & 0x0f) << 12) | ((buf[i + 1] & 0x3f) << 6) | (buf[i + 2] & 0x3f)
      );
    case 15:
      if (!(c & 0x8))
        // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        return (
          ((c & 0x07) << 18) |
          ((buf[i + 1] & 0x3f) << 12) |
          ((buf[i + 2] & 0x3f) << 6) |
          (buf[i + 3] & 0x3f)
        );
    default:
      throw new Error("Invalid UTF-8");
  }
}

export async function* read(
  file: FileHandle,
  position = 0,
  reverse = false,
  buffer?: Buffer
) {
  if (position < 0) position += (await file.stat()).size;

  buffer = buffer || Buffer.alloc(1 << 12);
  const size = buffer.length;
  let length = size;

  let pos = reverse ? position - size + 1 : position;

  const chars: number[] = Array(size * 2);

  let bytesRead = 0;
  let charCount = 0;

  let continuing = null;

  while (true) {
    if (!continuing) {
      charCount = 0;
      length = Math.min(size, size + pos);

      pos = Math.max(pos, 0);

      ({ bytesRead } = await file.read(buffer, 0, length, pos));

      for (let i = 0; i < bytesRead; ) {
        let index = reverse ? bytesRead - i - 1 : i;

        let count = 0;
        if (reverse) {
          count = 1;
          while ((buffer[index] & 0xc0) == 0x80) --index, ++count;
        } else {
          for (let b = 7; (buffer[index] >> b) & 1; b--) ++count;
          count = count || 1;
        }

        // Handle UTF-8 characters split at buffer boundary
        if (index < 0 || index + count > size)
          length -= reverse ? index + count : size - index;
        else {
          chars[charCount++] = pos + index;
          chars[charCount++] = utf8codepoint(buffer, index);
        }

        i += count;
      }
    }

    continuing = null;

    try {
      if (charCount == chars.length) yield chars;
      else yield chars.slice(0, charCount);
      // If we reached here, it means we yielded successfully and can update
      // the state for the next iteration.
      continuing = false;
      if (bytesRead < size || (reverse && !pos)) break;
      pos += reverse ? -length : length;
    } finally {
      if (continuing == null) {
        // If we reached here, then we didn't yield because the generator ended
        // prematurely, so remember to yield in the next iteration.
        continuing = true;
        continue;
      }
    }
  }
  return undefined;
}

// Allocating buffers is expensive, so preallocate one
const uint32Buffer = Buffer.alloc(4);
export function z85EncodeAsUInt32(number: number, compact = false) {
  uint32Buffer.writeUInt32BE(number, 0);
  const encoded = z85.encode(uint32Buffer);
  return compact ? encoded.replace(/^0+/, "") : encoded;
}

export function z85DecodeAsUInt32(string: string, compact = false) {
  if (string.length > 5)
    throw new Error("Cannot decode string longer than 5 characters");
  const decoded = z85.decode(compact ? string.padStart(5, "0") : string);
  return decoded.readUInt32BE(0);
}

const uintBuffer = Buffer.alloc(8);
export function z85EncodeAsUInt(number: number, compact = false) {
  uintBuffer.writeUIntBE(number, 2, 6);
  const encoded = z85.encode(uintBuffer);
  return compact ? encoded.replace(/^0+/, "") : encoded.slice(2);
}

export function z85DecodeAsUInt(string: string, compact = false) {
  if (!compact && string.length != 8)
    throw new Error("String must be 8 characters long");
  else if (string.length > 8)
    throw new Error("Cannot decode string longer than 8 characters");
  const decoded = z85.decode(string.padStart(10, "0"));
  return decoded.readUIntBE(2, 6);
}

const doubleBuffer = Buffer.alloc(8);
export function z85EncodeAsDouble(number: number, compact = false) {
  doubleBuffer.writeDoubleBE(number, 0);
  const encoded = z85.encode(doubleBuffer);
  return compact ? encoded.replace(/0+$/, "") : encoded;
}

export function z85DecodeAsDouble(string: string, compact = false) {
  if (string.length > 10)
    throw new Error("Cannot decode string longer than 5 characters");
  const decoded = z85.decode(compact ? string.padEnd(10, "0") : string);
  return decoded.readDoubleBE(0);
}

class Out extends Writable {
  constructor(protected label: string) {
    super();
  }
  _write(chunk: string, _: string, callback: (error?: Error | null) => void) {
    process.stdout.write(`${this.label}: ${chunk}`);
    if (callback) callback(null);
  }
}
class Err extends Writable {
  constructor(protected label: string) {
    super();
  }
  _write(chunk: string, _: string, callback: (error?: Error | null) => void) {
    process.stderr.write(`${this.label}: ${chunk}`);
    if (callback) callback(null);
  }
}

const dummyConsole = new Console(new Writable());

export function logger(name: string, log?: boolean) {
  if (log == null) log = Boolean(process.env.DEBUG);
  return log ? new Console(new Out(name), new Err(name)) : dummyConsole;
}
