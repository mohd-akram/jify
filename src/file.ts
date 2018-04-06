import * as fs from 'fs';
import { promisify } from 'util';

import { read } from './utils';

const fsOpen = promisify(fs.open);
const fsClose = promisify(fs.close);
const fsWrite = promisify(fs.write);

class File {
  protected fd: number | null = null;

  constructor(protected filename: string) { }

  get isOpen() {
    return this.fd != null;
  }

  read(position: number, reverse = false) {
    if (!this.fd)
      throw new Error('Need to call open() before read()');
    return read(this.fd, position, reverse);
  }

  async write(position: number, text: string) {
    const alreadyOpen = this.isOpen;
    if (!alreadyOpen)
      await this.open();
    await fsWrite(this.fd!, text, position);
    if (!alreadyOpen)
      await this.close();
  }

  async clear(position: number, length: number, char = ' ') {
    const alreadyOpen = this.isOpen;
    if (!alreadyOpen)
      await this.open();
    const buffer = Buffer.alloc(length, char);
    await fsWrite(this.fd!, buffer, 0, length, position);
    if (!alreadyOpen)
      await this.close();
  }

  async truncate(position: number) {
    const alreadyOpen = this.isOpen;
    if (!alreadyOpen)
      await this.open();
    await promisify(fs.ftruncate)(this.fd!, position);
    if (!alreadyOpen)
      await this.close();
  }

  async exists() {
    return await promisify(fs.exists)(this.filename);
  }

  async append(text: string) {
    await promisify(fs.appendFile)(this.filename, text);
  }

  async delete() {
    await promisify(fs.unlink)(this.filename);
  }

  async open(mode = 'r+') {
    if (this.isOpen)
      throw new Error('File already open');
    this.fd = await fsOpen(this.filename, mode);
  }

  async close() {
    if (!this.fd)
      throw new Error('No open file to close');
    await fsClose(this.fd);
    this.fd = null;
  }
}

export default File;