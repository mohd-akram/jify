import { EventEmitter } from 'events';
import { promises as fs } from 'fs';
import { FileHandle } from 'fs/promises';

import { lock, unlock } from 'os-lock';

import { logger, read } from './utils';

class File extends EventEmitter {
  protected file: FileHandle | null = null;
  protected lockedPositions =
    new Map<number, { exclusive: boolean, count: number }>();
  protected logger = logger('file');

  protected reads = 0;
  protected writes = 0;

  constructor(protected filename: string) {
    super();
  }

  get isOpen() {
    return this.file != null;
  }

  read(position: number, reverse = false, buffer?: Buffer) {
    ++this.reads;
    if (!this.file)
      throw new Error('Need to call open() before read()');
    return read(this.file, position, reverse, buffer);
  }

  async write(position: number, buffer: Buffer) {
    ++this.writes;
    await this.file!.write(buffer, 0, buffer.length, position);
  }

  async clear(position: number, length: number, char = ' ') {
    const buffer = Buffer.alloc(length, char);
    await this.file!.write(buffer, 0, length, position);
  }

  async truncate(position: number) {
    await this.file!.truncate(position);
  }

  async append(text: string) {
    await fs.appendFile(this.filename, text);
  }

  async delete() {
    await fs.unlink(this.filename);
  }

  async stat() {
    return await fs.stat(this.filename);
  }

  async open(mode = 'r+') {
    this.logger.log('opening', this.filename);
    if (this.isOpen)
      throw new Error('File already open');
    this.file = await fs.open(this.filename, mode);
  }

  async close() {
    this.logger.log('closing', this.filename);
    this.logger.log('reads', this.reads);
    this.logger.log('writes', this.writes);
    if (!this.file)
      throw new Error('No open file to close');
    await this.file.close();
    this.file = null;
    this.reads = 0;
    this.writes = 0;
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
      await lock(this.file!.fd, pos, 1, options);
      return;
    } else {
      const timeout = setInterval(() => { }, ~0 >>> 1);
      await new Promise<void>(resolve => {
        this.once('unlock', async () => {
          clearInterval(timeout);
          await this.lock(pos, options);
          resolve();
        });
      });
    }
  }

  async unlock(pos = 0) {
    await unlock(this.file!.fd, pos, 1);
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
