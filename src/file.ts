import { EventEmitter } from 'events';
import * as fs from 'fs';
import { promisify } from 'util';

import { lock, unlock } from 'os-lock';

import { read } from './utils';

const fsOpen = promisify(fs.open);
const fsClose = promisify(fs.close);
const fsWrite = promisify(fs.write);

class File extends EventEmitter {
  protected fd: number | null = null;
  protected lockedPositions =
    new Map<number, { exclusive: boolean, count: number }>();

  constructor(protected filename: string) {
    super();
  }

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

  async stat() {
    return await promisify(fs.stat)(this.filename);
  }

  async open(mode = 'r+') {
    if (this.isOpen)
      throw new Error('File already open');
    this.fd = await fsOpen(this.filename, mode);
  }

  openSync(mode = 'r+') {
    if (this.isOpen)
      throw new Error('File already open');
    this.fd = fs.openSync(this.filename, mode);
  }

  async close() {
    if (!this.fd)
      throw new Error('No open file to close');
    await fsClose(this.fd);
    this.fd = null;
  }

  closeSync() {
    if (!this.fd)
      throw new Error('No open file to close');
    fs.closeSync(this.fd);
  }

  async lock(pos = 0, options = { exclusive: false }) {
    const lockedPosition = this.lockedPositions.get(pos) ||
      { count: 0, exclusive: false };

    const canGetLock = options.exclusive ?
      !lockedPosition.count : !lockedPosition.exclusive;

    if (canGetLock) {
      ++lockedPosition.count;
      lockedPosition.exclusive = options.exclusive;
      this.lockedPositions.set(pos, lockedPosition);
      await lock(this.fd!, pos, 1, options);
      return;
    } else {
      return new Promise(resolve => {
        this.once('unlock', () => {
          this.lock(pos, options).then(() => resolve());
        });
      });
    }
  }

  async unlock(pos = 0) {
    await unlock(this.fd!, pos, 1);
    const lockedPosition = this.lockedPositions.get(pos)!;
    lockedPosition.exclusive = false;
    --lockedPosition.count;
    if (!lockedPosition.count) {
      this.lockedPositions.delete(pos);
      this.emit('unlock', pos);
    }
  }
}

export default File;
