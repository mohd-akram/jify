import * as fs from 'fs';

import * as z85 from 'z85';

export async function readJSON(
  stream: IterableIterator<[number, string]>, parse = true
) {
  const chars = [];

  let type: string | undefined;
  let start = -1;
  let length = 0;
  let depth = 0;
  let inString = false;
  let prevChar;

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
  let prevChar;
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

export function* read(fd: number, position = 0, reverse = false) {
  // This function is synchronous because async generators are still too slow
  if (position < 0)
    position += fs.fstatSync(fd).size;

  const size = 1 << 12;
  const buffer = Buffer.alloc(size);

  let pos = reverse ? position - size + 1 : position;

  while (true) {
    const length = Math.min(size, size + pos);

    pos = Math.max(pos, 0);

    const bytesRead = fs.readSync(fd, buffer, 0, length, pos);

    for (let i = 0; i < bytesRead; i++) {
      const index = reverse ? (bytesRead - i - 1) : i;
      yield [
        pos + index,
        String.fromCharCode(buffer[index])
      ] as [number, string];
    }

    if (bytesRead < size || (reverse && !pos))
      break;

    pos += reverse ? -size : size;
  }
}

export function z85EncodeAsUInt32(number: number) {
  const buffer = Buffer.alloc(4);
  buffer.writeUInt32BE(number, 0);
  return z85.encode(buffer);
}

export function z85DecodeAsUInt32(string: string) {
  if (string.length > 5)
    throw new Error('Cannot decode string longer than 5 characters');
  return z85.decode(string.padStart(5, '0')).readUInt32BE(0);
}
