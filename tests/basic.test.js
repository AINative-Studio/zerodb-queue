/**
 * @zerodb/queue — unit tests
 *
 * Uses Node.js built-in test runner (zero deps).
 * Mocks the ZeroDB HTTP layer so tests run offline.
 */

import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';

// Mock fetch globally before importing the module
let fetchCalls = [];
let fetchResponses = [];

function pushResponse(body, status = 200) {
  fetchResponses.push({ body, status });
}

function mockFetch(url, opts) {
  const call = { url, opts, body: opts?.body ? JSON.parse(opts.body) : null };
  fetchCalls.push(call);

  const resp = fetchResponses.shift() || { body: { success: true }, status: 200 };
  return Promise.resolve({
    ok: resp.status >= 200 && resp.status < 300,
    status: resp.status,
    json: () => Promise.resolve(resp.body),
    text: () => Promise.resolve(JSON.stringify(resp.body)),
  });
}

// Set env before import
process.env.ZERODB_API_TOKEN = 'test-token-123';
process.env.ZERODB_PROJECT_ID = 'test-project';

// Patch global fetch
const originalFetch = globalThis.fetch;
globalThis.fetch = mockFetch;

// Dynamic import after patching
const { Queue, Worker, Job, JobStatus, QueueScheduler, FlowProducer, QueueError, ZeroDBError, MiniEmitter, ZeroDBClient } = await import('../src/index.js');

describe('ZeroDBClient', () => {
  beforeEach(() => {
    fetchCalls = [];
    fetchResponses = [];
  });

  it('throws without API token', () => {
    const orig = process.env.ZERODB_API_TOKEN;
    delete process.env.ZERODB_API_TOKEN;
    assert.throws(() => new ZeroDBClient({}), /ZERODB_API_TOKEN is required/);
    process.env.ZERODB_API_TOKEN = orig;
  });

  it('uses env vars for config', () => {
    const client = new ZeroDBClient();
    assert.equal(client.apiToken, 'test-token-123');
    assert.equal(client.projectId, 'test-project');
  });

  it('uses explicit opts over env vars', () => {
    const client = new ZeroDBClient({ apiToken: 'custom', projectId: 'proj2' });
    assert.equal(client.apiToken, 'custom');
    assert.equal(client.projectId, 'proj2');
  });

  it('calls ZeroDB execute endpoint', async () => {
    pushResponse({ ok: true });
    const client = new ZeroDBClient();
    await client.execute('create_table', { table_name: 'test' });

    assert.equal(fetchCalls.length, 1);
    assert.ok(fetchCalls[0].url.includes('/v1/public/zerodb/mcp/execute'));
    assert.equal(fetchCalls[0].body.operation, 'create_table');
    assert.equal(fetchCalls[0].body.params.project_id, 'test-project');
  });

  it('throws ZeroDBError on HTTP failure', async () => {
    pushResponse({ error: 'not found' }, 404);
    const client = new ZeroDBClient();
    await assert.rejects(() => client.execute('get_table', {}), ZeroDBError);
  });
});

describe('Job', () => {
  let queue;

  beforeEach(() => {
    fetchCalls = [];
    fetchResponses = [];
    // Provision response
    pushResponse({ success: true });
    queue = new Queue('test-queue');
  });

  it('creates a job with correct defaults', () => {
    const job = new Job(queue, 'send-email', { to: 'a@b.com' });
    assert.equal(job.name, 'send-email');
    assert.deepEqual(job.data, { to: 'a@b.com' });
    assert.equal(job.status, JobStatus.WAITING);
    assert.equal(job.progress, 0);
    assert.equal(job.attemptsMade, 0);
    assert.ok(job.id);
    assert.ok(job.timestamp > 0);
  });

  it('respects custom jobId', () => {
    const job = new Job(queue, 'test', {}, { jobId: 'custom-123' });
    assert.equal(job.id, 'custom-123');
  });

  it('serializes to row and back', () => {
    const job = new Job(queue, 'process', { items: [1, 2] }, { priority: 5 });
    const row = job.toRow();
    assert.equal(row.job_name, 'process');
    assert.equal(row.priority, 5);
    assert.equal(typeof row.data, 'string');

    const restored = Job.fromRow(queue, row);
    assert.equal(restored.name, 'process');
    assert.deepEqual(restored.data, { items: [1, 2] });
    assert.equal(restored.priority, 5);
  });

  it('handles null result in row', () => {
    const row = {
      id: 'x',
      job_name: 'test',
      data: '{}',
      opts: '{}',
      status: 'waiting',
      progress: 0,
      result: null,
      error: null,
      attempts_made: 0,
      max_attempts: 3,
      priority: 0,
      delay: 0,
      timestamp: Date.now(),
      processed_on: null,
      finished_on: null,
      stacktrace: null,
    };
    const job = Job.fromRow(queue, row);
    assert.equal(job.result, null);
    assert.deepEqual(job.stacktrace, []);
  });
});

describe('JobStatus', () => {
  it('has all expected statuses', () => {
    assert.equal(JobStatus.WAITING, 'waiting');
    assert.equal(JobStatus.ACTIVE, 'active');
    assert.equal(JobStatus.COMPLETED, 'completed');
    assert.equal(JobStatus.FAILED, 'failed');
    assert.equal(JobStatus.DELAYED, 'delayed');
    assert.equal(JobStatus.STALLED, 'stalled');
  });

  it('is frozen', () => {
    assert.ok(Object.isFrozen(JobStatus));
  });
});

describe('Queue', () => {
  beforeEach(() => {
    fetchCalls = [];
    fetchResponses = [];
  });

  it('throws on invalid name', () => {
    assert.throws(() => new Queue(''), /Queue name is required/);
    assert.throws(() => new Queue(123), /Queue name is required/);
  });

  it('sanitizes table name', () => {
    const q = new Queue('my-queue.v2');
    assert.equal(q._tableName, 'queue_my_queue_v2');
  });

  it('provisions table on first add', async () => {
    // create_table response
    pushResponse({ success: true });
    // insert_rows response
    pushResponse({ success: true });
    // create_event response
    pushResponse({ success: true });

    const q = new Queue('emails');
    const job = await q.add('send', { to: 'test@example.com' });

    assert.equal(job.name, 'send');
    assert.equal(job.status, JobStatus.WAITING);

    // First call = create_table, second = insert_rows
    assert.equal(fetchCalls[0].body.operation, 'create_table');
    assert.equal(fetchCalls[1].body.operation, 'insert_rows');
  });

  it('provisions table only once', async () => {
    pushResponse({ success: true }); // create_table
    pushResponse({ success: true }); // insert_rows
    pushResponse({ success: true }); // create_event
    pushResponse({ success: true }); // insert_rows (second add)
    pushResponse({ success: true }); // create_event (second add)

    const q = new Queue('emails');
    await q.add('send1', {});
    await q.add('send2', {});

    const createTableCalls = fetchCalls.filter((c) => c.body.operation === 'create_table');
    assert.equal(createTableCalls.length, 1);
  });

  it('handles table already exists gracefully', async () => {
    pushResponse({ error: 'already exists' }, 409);
    pushResponse({ success: true }); // insert_rows
    pushResponse({ success: true }); // create_event

    const q = new Queue('existing');
    const job = await q.add('test', { x: 1 });
    assert.equal(job.name, 'test');
  });

  it('adds delayed jobs', async () => {
    pushResponse({ success: true }); // create_table
    pushResponse({ success: true }); // insert_rows
    pushResponse({ success: true }); // create_event

    const q = new Queue('delayed');
    const job = await q.add('later', {}, { delay: 5000 });
    assert.equal(job.status, JobStatus.DELAYED);
    assert.equal(job.delay, 5000);
  });

  it('addBulk inserts multiple jobs', async () => {
    pushResponse({ success: true }); // create_table
    pushResponse({ success: true }); // insert_rows (bulk)

    const q = new Queue('bulk');
    const jobs = await q.addBulk([
      { name: 'a', data: { v: 1 } },
      { name: 'b', data: { v: 2 } },
      { name: 'c', data: { v: 3 } },
    ]);

    assert.equal(jobs.length, 3);
    const insertCall = fetchCalls.find((c) => c.body.operation === 'insert_rows');
    assert.equal(insertCall.body.params.rows.length, 3);
  });

  it('getJob returns null for missing job', async () => {
    pushResponse({ success: true }); // create_table
    pushResponse({ rows: [] }); // query_rows

    const q = new Queue('empty');
    const job = await q.getJob('nonexistent');
    assert.equal(job, null);
  });

  it('getJob returns a Job instance', async () => {
    pushResponse({ success: true }); // create_table
    pushResponse({
      rows: [{
        id: 'job-1',
        job_name: 'send',
        data: '{"to":"a@b.com"}',
        opts: '{}',
        status: 'completed',
        progress: 100,
        result: '{"sent":true}',
        error: null,
        attempts_made: 1,
        max_attempts: 3,
        priority: 0,
        delay: 0,
        timestamp: Date.now(),
        processed_on: Date.now(),
        finished_on: Date.now(),
        stacktrace: '[]',
      }],
    });

    const q = new Queue('test');
    const job = await q.getJob('job-1');
    assert.ok(job instanceof Job);
    assert.equal(job.id, 'job-1');
    assert.equal(job.status, 'completed');
    assert.deepEqual(job.result, { sent: true });
  });

  it('getJobs queries by status', async () => {
    pushResponse({ success: true }); // create_table
    pushResponse({ rows: [] });

    const q = new Queue('test');
    const jobs = await q.getJobs('waiting');
    assert.ok(Array.isArray(jobs));
    assert.equal(jobs.length, 0);

    const queryCall = fetchCalls.find((c) => c.body.operation === 'query_rows');
    assert.deepEqual(queryCall.body.params.filters.status, { $in: ['waiting'] });
  });

  it('remove deletes a job', async () => {
    pushResponse({ success: true }); // create_table
    pushResponse({ success: true }); // delete_rows

    const q = new Queue('test');
    await q.remove('job-1');

    const deleteCall = fetchCalls.find((c) => c.body.operation === 'delete_rows');
    assert.equal(deleteCall.body.params.filters.id, 'job-1');
  });

  it('drain removes all jobs', async () => {
    pushResponse({ success: true }); // create_table
    pushResponse({ success: true }); // delete_rows

    const q = new Queue('test');
    await q.drain();

    const deleteCall = fetchCalls.find((c) => c.body.operation === 'delete_rows');
    assert.equal(deleteCall.body.params.filters.queue_name, 'test');
  });

  it('obliterate deletes the table', async () => {
    pushResponse({ success: true }); // delete_table

    const q = new Queue('doomed');
    q._provisioned = true;
    await q.obliterate();

    assert.equal(q._provisioned, false);
    const deleteCall = fetchCalls.find((c) => c.body.operation === 'delete_table');
    assert.ok(deleteCall);
  });

  it('emits added event', async () => {
    pushResponse({ success: true }); // create_table
    pushResponse({ success: true }); // insert_rows
    pushResponse({ success: true }); // create_event

    const q = new Queue('events');
    let emitted = null;
    q.on('added', (job) => { emitted = job; });
    await q.add('test', { x: 1 });
    assert.ok(emitted);
    assert.equal(emitted.name, 'test');
  });
});

describe('MiniEmitter', () => {
  it('on/emit works', () => {
    const e = new MiniEmitter();
    let called = false;
    e.on('test', () => { called = true; });
    e.emit('test');
    assert.ok(called);
  });

  it('passes arguments', () => {
    const e = new MiniEmitter();
    let args;
    e.on('test', (a, b) => { args = [a, b]; });
    e.emit('test', 1, 2);
    assert.deepEqual(args, [1, 2]);
  });

  it('off removes listener', () => {
    const e = new MiniEmitter();
    let count = 0;
    const fn = () => { count++; };
    e.on('test', fn);
    e.emit('test');
    e.off('test', fn);
    e.emit('test');
    assert.equal(count, 1);
  });

  it('once fires only once', () => {
    const e = new MiniEmitter();
    let count = 0;
    e.once('test', () => { count++; });
    e.emit('test');
    e.emit('test');
    assert.equal(count, 1);
  });
});

describe('Worker', () => {
  beforeEach(() => {
    fetchCalls = [];
    fetchResponses = [];
  });

  it('throws on invalid processor', () => {
    assert.throws(() => new Worker('test', null, { autorun: false }), /processor must be a function/);
  });

  it('creates a queue internally', () => {
    const w = new Worker('test-q', async () => {}, { autorun: false });
    assert.ok(w.queue instanceof Queue);
    assert.equal(w.queue.name, 'test-q');
  });

  it('processes a job successfully', async () => {
    const w = new Worker('w-proc', async (job) => {
      return { sent: true, to: job.data.to };
    }, { autorun: false, pollInterval: 30 });

    // Pre-provision to avoid create_table call
    w.queue._provisioned = true;

    // Mock responses: delayed query, waiting query, active update, completed update, then empties
    pushResponse({ rows: [] }); // query delayed
    pushResponse({ rows: [{ // query waiting
      id: 'j1', job_name: 'send', data: '{"to":"a@b.com"}', opts: '{}',
      status: 'waiting', progress: 0, result: null, error: null,
      attempts_made: 0, max_attempts: 3, priority: 0, delay: 0,
      timestamp: Date.now(), processed_on: null, finished_on: null, stacktrace: '[]',
    }] });
    pushResponse({ success: true }); // update to active
    pushResponse({ success: true }); // update to completed
    for (let i = 0; i < 10; i++) pushResponse({ rows: [], success: true });

    const done = new Promise((resolve) => {
      w.on('completed', (job) => { w.close().then(() => resolve(job)); });
      w.on('error', () => {});
      w.run();
      setTimeout(() => { w.close().then(() => resolve(null)); }, 2000);
    });

    const job = await done;
    assert.ok(job, 'Worker should have completed a job');
    assert.equal(job.id, 'j1');
  });

  it('emits completed event', async () => {
    const w = new Worker('w-completed', async () => 'done', { autorun: false, pollInterval: 30 });
    w.queue._provisioned = true;

    pushResponse({ rows: [] }); // delayed
    pushResponse({ rows: [{ // waiting
      id: 'j2', job_name: 'x', data: '{}', opts: '{}',
      status: 'waiting', progress: 0, result: null, error: null,
      attempts_made: 0, max_attempts: 3, priority: 0, delay: 0,
      timestamp: Date.now(), processed_on: null, finished_on: null, stacktrace: '[]',
    }] });
    pushResponse({ success: true }); // active
    pushResponse({ success: true }); // completed
    for (let i = 0; i < 10; i++) pushResponse({ rows: [], success: true });

    const done = new Promise((resolve) => {
      w.on('completed', () => { w.close().then(() => resolve(true)); });
      w.on('error', () => {});
      w.run();
      setTimeout(() => { w.close().then(() => resolve(false)); }, 2000);
    });

    assert.ok(await done, 'completed event should have fired');
  });

  it('retries failed jobs', async () => {
    const w = new Worker('w-retry', async () => { throw new Error('fail'); }, { autorun: false, pollInterval: 30 });
    w.queue._provisioned = true;

    pushResponse({ rows: [] }); // delayed
    pushResponse({ rows: [{ // waiting
      id: 'j3', job_name: 'flaky', data: '{}', opts: '{}',
      status: 'waiting', progress: 0, result: null, error: null,
      attempts_made: 0, max_attempts: 3, priority: 0, delay: 0,
      timestamp: Date.now(), processed_on: null, finished_on: null, stacktrace: '[]',
    }] });
    pushResponse({ success: true }); // active
    pushResponse({ success: true }); // update to delayed (retry)
    for (let i = 0; i < 10; i++) pushResponse({ rows: [], success: true });

    const done = new Promise((resolve) => {
      w.on('retrying', () => { w.close().then(() => resolve(true)); });
      w.on('error', () => {});
      w.run();
      setTimeout(() => { w.close().then(() => resolve(false)); }, 2000);
    });

    assert.ok(await done, 'retrying event should have fired');
  });

  it('emits failed after max attempts', async () => {
    const w = new Worker('w-fail', async () => { throw new Error('permanent'); }, { autorun: false, pollInterval: 30 });
    w.queue._provisioned = true;

    pushResponse({ rows: [] }); // delayed
    pushResponse({ rows: [{ // waiting — already at 2/3 attempts
      id: 'j4', job_name: 'doomed', data: '{}', opts: '{}',
      status: 'waiting', progress: 0, result: null, error: null,
      attempts_made: 2, max_attempts: 3, priority: 0, delay: 0,
      timestamp: Date.now(), processed_on: null, finished_on: null, stacktrace: '[]',
    }] });
    pushResponse({ success: true }); // active
    pushResponse({ success: true }); // failed
    for (let i = 0; i < 10; i++) pushResponse({ rows: [], success: true });

    const done = new Promise((resolve) => {
      w.on('failed', (job, err) => {
        assert.equal(err.message, 'permanent');
        w.close().then(() => resolve(true));
      });
      w.on('error', () => {});
      w.run();
      setTimeout(() => { w.close().then(() => resolve(false)); }, 2000);
    });

    assert.ok(await done, 'failed event should have fired');
  });

  it('pause and resume', async () => {
    const w = new Worker('test', async () => {}, { autorun: false, pollInterval: 50 });
    w.run();
    assert.equal(w.running, true);
    assert.equal(w.paused, false);

    w.pause();
    assert.equal(w.paused, true);

    w.resume();
    assert.equal(w.paused, false);

    await w.close();
  });

  it('close emits closed event', async () => {
    const w = new Worker('test', async () => {}, { autorun: false });
    let closed = false;
    w.on('closed', () => { closed = true; });
    await w.close();
    assert.ok(closed);
  });
});

describe('FlowProducer', () => {
  beforeEach(() => {
    fetchCalls = [];
    fetchResponses = [];
  });

  it('adds parent and children', async () => {
    // Child queue provision
    pushResponse({ success: true }); // create_table
    pushResponse({ success: true }); // insert_rows
    pushResponse({ success: true }); // create_event
    // Second child
    pushResponse({ success: true }); // insert_rows
    pushResponse({ success: true }); // create_event
    // Parent queue provision
    pushResponse({ success: true }); // create_table
    pushResponse({ success: true }); // insert_rows
    pushResponse({ success: true }); // create_event

    const flow = new FlowProducer();
    const result = await flow.add({
      name: 'parent-job',
      queueName: 'parent-q',
      data: { type: 'aggregate' },
      children: [
        { name: 'child-1', queueName: 'child-q', data: { step: 1 } },
        { name: 'child-2', queueName: 'child-q', data: { step: 2 } },
      ],
    });

    assert.equal(result.job.name, 'parent-job');
    assert.equal(result.children.length, 2);
    assert.ok(result.job.data._childJobIds);
    assert.equal(result.job.data._childJobIds.length, 2);
  });
});

describe('QueueError', () => {
  it('has correct name and code', () => {
    const err = new QueueError('test error', 'TEST_CODE');
    assert.equal(err.name, 'QueueError');
    assert.equal(err.code, 'TEST_CODE');
    assert.equal(err.message, 'test error');
    assert.ok(err instanceof Error);
  });
});

describe('ZeroDBError', () => {
  it('extends QueueError', () => {
    const err = new ZeroDBError('bad request', 400);
    assert.ok(err instanceof QueueError);
    assert.ok(err instanceof Error);
    assert.equal(err.status, 400);
    assert.equal(err.code, 'ZERODB_ERROR');
  });
});

// Cleanup
afterEach(() => {
  fetchCalls = [];
  fetchResponses = [];
});
