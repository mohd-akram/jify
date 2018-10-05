import { EventEmitter } from 'events';
import * as fs from 'fs';
import { promisify } from 'util';

import { lock, unlock } from 'os-lock';

import { logger, read } from './utils';

const fsOpen = promisify(fs.open);
const fsClose = promisify(fs.close);
const fsWrite = promisify(fs.write);

class File extends EventEmitter {
  protected fd: number | null = null;
  protected lockedPositions =
    new Map<number, { exclusive: boolean, count: number }>();
  protected logger = logger('file');

  protected reads = 0;
  protected writes = 0;

  constructor(protected filename: string) {
    super();
  }

  get isOpen() {
    return this.fd != null;
  }

  read(position: number, reverse = false, buffer?: Buffer) {
    ++this.reads;
    if (!this.fd)
      throw new Error('Need to call open() before read()');
    return read(this.fd, position, reverse, buffer);
  }

  async write(position: number, buffer: Buffer) {
    ++this.writes;
    await fsWrite(this.fd!, buffer, 0, buffer.length, position);
  }

  async clear(position: number, length: number, char = ' ') {
    const buffer = Buffer.alloc(length, char);
    await fsWrite(this.fd!, buffer, 0, length, position);
  }

  async truncate(position: number) {
    await promisify(fs.ftruncate)(this.fd!, position);
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
    this.logger.log('opening', this.filename);
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
    this.logger.log('closing', this.filename);
    this.logger.log('reads', this.reads);
    this.logger.log('writes', this.writes);
    if (!this.fd)
      throw new Error('No open file to close');
    await fsClose(this.fd);
    this.fd = null;
    this.reads = 0;
    this.writes = 0;
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
      const timeout = setInterval(() => { }, ~0 >>> 1);
      await new Promise(resolve => {
        this.once('unlock', async () => {
          clearInterval(timeout);
          await this.lock(pos, options);
          resolve();
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
