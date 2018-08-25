import * as child_process from 'child_process';

import Index, { ObjectField } from './index';

function waitForClose(subprocess: child_process.ChildProcess) {
  return new Promise(resolve => {
    subprocess.once('close', () => resolve());
  });
}

function waitForReady(subprocess: child_process.ChildProcess) {
  return new Promise(resolve => {
    subprocess.once('message', message => {
      if (message == 'ready')
        resolve();
    });
  });
}

const batchSize = 1_000_000;

async function main(args: string[]) {
  const [filename, fieldName, ...fieldNames] = args.slice(2);

  const subprocesses: { [field: string]: child_process.ChildProcess } = {};
  const batches: { [field: string]: ObjectField[] } = { [fieldName]: [] };

  for (const name of fieldNames) {
    subprocesses[name] = child_process.fork(__filename, [filename, name]);
    batches[name] = [];
  }

  await Promise.all(Object.values(subprocesses).map(waitForReady));

  const index = new Index(filename);
  await index.open();
  const cache = new Map();

  function insertBatch(name: string) {
    const batch = batches[name];
    batches[name] = [];
    if (batch.length)
      index.insert(batch, cache);
  }

  function sendBatch(name: string) {
    const batch = batches[name];
    batches[name] = [];
    if (batch.length)
      subprocesses[name].send(batch);
  }

  const handler = async (objectFields?: ObjectField[]) => {
    if (objectFields == null) {
      // Insert remaining fields
      insertBatch(fieldName);
      // Send remaining batches
      for (const name in subprocesses)
        sendBatch(name);
      // Wait for other processes to close
      for (const subprocess of Object.values(subprocesses))
        subprocess.send(null);
      await Promise.all(Object.values(subprocesses).map(waitForClose));
      // Cleanup
      process.off('message', handler);
      process.once('beforeExit', async () => {
        await index.close();
      });
      return;
    }

    if (!objectFields.length)
      return;

    if (fieldNames.length) {
      for (const field of objectFields) {
        const batch = batches[field.name];
        batch.push(field);
        if (batch.length >= batchSize) {
          if (field.name == fieldName)
            insertBatch(field.name);
          else
            sendBatch(field.name);
        }
      }
    } else {
      // Insert immediately if possible
      if (objectFields.length >= batchSize)
        index.insert(objectFields, cache);
      else {
        const batch = batches[fieldName];
        for (const field of objectFields) {
          batch.push(field);
          if (batch.length >= batchSize)
            insertBatch(fieldName);
        }
      }
    }
  };
  process.on('message', handler);
  if (process.send)
    process.send('ready');
}

main(process.argv);
