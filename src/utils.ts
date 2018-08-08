import * as fs from 'fs';

import * as z85 from 'z85';

export function readJSONSync(
  stream: IterableIterator<[number, string]>, parse = true
) {
  const chars = [];

  let type: string | undefined;
  let start = -1;
  let length = 0;
  let depth = 0;
  let inString = false;
  let prevChar: string | undefined;

  for (const [i, char] of stream) {
    if (start == -1) {
      if (char == ' ' || char == '\n')
        continue;
      else {
        start = i;
        if (char == '{')
          type = 'object';
        else if (char == '"')
          type = 'string';
        else if (char == '[')
          type = 'array';
      }
    }

    const isStringQuote = char == '"' && prevChar != '\\';

    if (isStringQuote)
      inString = !inString;

    ++length;
    if (parse)
      chars.push(char);
    prevChar = char;

    if (inString && type != 'string')
      continue;

    switch (type) {
      case 'array':
        if (char == '[')
          ++depth;
        else if (char == ']')
          --depth;
        break;
      case 'object':
        if (char == '{')
          ++depth;
        else if (char == '}')
          --depth;
        break;
      case 'string':
        if (isStringQuote)
          depth = Number(!depth);
        break;
      default:
        if (
          char == ' ' || char == '\n' || char == ',' ||
          char == '}' || char == ']'
        )
          --depth;
        else if (!depth)
          ++depth;
    }

    if (!depth)
      break;
  }

  if (start == -1)
    throw new Error('No JSON object found');

  return parse ?
    { value: JSON.parse(chars.join('')), start, length } :
    { start, length };
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

export function* readSync(
  fd: number, position = 0, reverse = false, buffer?: Buffer
) {
  // This function is synchronous because async generators are still too slow
  if (position < 0)
    position += fs.fstatSync(fd).size;

  buffer = buffer || Buffer.alloc(1 << 16);
  const size = buffer.length;

  let pos = reverse ? position - size + 1 : position;

  while (true) {
    const length = Math.min(size, size + pos);

    pos = Math.max(pos, 0);

    const bytesRead = fs.readSync(fd, buffer, 0, length, pos);

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

      try {
        yield [
          pos + index,
          buffer.toString('utf-8', index, index + count)
        ] as [number, string];
        i += count;
      } finally {
        // Prevent generator from being marked as done prematurely
        continue;
      }
    }

    if (bytesRead < size || (reverse && !pos))
      break;

    pos += reverse ? -size : size;
  }
}

// Allocating buffers is expensive, so preallocate one
const uint32Buffer = Buffer.alloc(4);
export function z85EncodeAsUInt32(number: number, pad = true) {
  uint32Buffer.writeUInt32BE(number, 0);
  const encoded = z85.encode(uint32Buffer);
  return pad ? encoded : (encoded.replace(/^0+/, '') || '0');
}

export function z85DecodeAsUInt32(string: string) {
  if (string.length > 5)
    throw new Error('Cannot decode string longer than 5 characters');
  return z85.decode(string.padStart(5, '0')).readUInt32BE(0);
}

const doubleBuffer = Buffer.alloc(8);
export function z85EncodeAsDouble(number: number, pad = true) {
  doubleBuffer.writeDoubleBE(number, 0);
  const encoded = z85.encode(doubleBuffer);
  return pad ? encoded : (encoded.replace(/0+$/, '') || '0');
}

export function z85DecodeAsDouble(string: string) {
  if (string.length > 10)
    throw new Error('Cannot decode string longer than 5 characters');
  return z85.decode(string.padEnd(10, '0')).readDoubleBE(0);
}
