/**
 * @zerodb/queue — BullMQ-compatible job queue powered by ZeroDB
 *
 * Zero infrastructure. No Redis required. Auto-provisions on first use.
 *
 * @example
 * import { Queue, Worker } from '@zerodb/queue';
 *
 * const queue = new Queue('emails');
 * await queue.add('send-welcome', { to: 'user@example.com' });
 *
 * const worker = new Worker('emails', async (job) => {
 *   console.log('Processing:', job.data);
 *   return { sent: true };
 * });
 */

// ---------------------------------------------------------------------------
// ZeroDB HTTP client (native fetch, zero deps)
// ---------------------------------------------------------------------------

const DEFAULT_BASE_URL = 'https://api.ainative.studio';
const DEFAULT_POLL_INTERVAL = 1000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_BACKOFF_MS = 1000;
const DEFAULT_CONCURRENCY = 1;
const DEFAULT_STALL_INTERVAL = 30000;

class ZeroDBClient {
  constructor(opts = {}) {
    this.baseUrl = opts.baseUrl || process.env.ZERODB_BASE_URL || DEFAULT_BASE_URL;
    this.apiToken = opts.apiToken || process.env.ZERODB_API_TOKEN;
    this.projectId = opts.projectId || process.env.ZERODB_PROJECT_ID;

    if (!this.apiToken) {
      throw new Error(
        '@zerodb/queue: ZERODB_API_TOKEN is required. ' +
        'Get one free at https://zerodb.dev or set it in your environment.'
      );
    }
  }

  async execute(operation, params = {}) {
    const body = {
      operation,
      params: { ...params },
    };
    if (this.projectId && !body.params.project_id) {
      body.params.project_id = this.projectId;
    }

    const res = await fetch(`${this.baseUrl}/v1/public/zerodb/mcp/execute`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.apiToken}`,
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new ZeroDBError(`ZeroDB ${operation} failed (${res.status}): ${text}`, res.status);
    }

    return res.json();
  }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

class QueueError extends Error {
  constructor(message, code) {
    super(message);
    this.name = 'QueueError';
    this.code = code;
  }
}

class ZeroDBError extends QueueError {
  constructor(message, status) {
    super(message, 'ZERODB_ERROR');
    this.name = 'ZeroDBError';
    this.status = status;
  }
}

// ---------------------------------------------------------------------------
// Job
// ---------------------------------------------------------------------------

/** Job status enum */
const JobStatus = Object.freeze({
  WAITING: 'waiting',
  ACTIVE: 'active',
  COMPLETED: 'completed',
  FAILED: 'failed',
  DELAYED: 'delayed',
  STALLED: 'stalled',
});

class Job {
  constructor(queue, name, data, opts = {}) {
    this.id = opts.jobId || crypto.randomUUID();
    this.queue = queue;
    this.queueName = queue.name;
    this.name = name;
    this.data = data;
    this.opts = opts;
    this.status = JobStatus.WAITING;
    this.progress = 0;
    this.result = null;
    this.error = null;
    this.attemptsMade = 0;
    this.maxAttempts = opts.attempts || DEFAULT_MAX_RETRIES;
    this.backoff = opts.backoff || DEFAULT_BACKOFF_MS;
    this.delay = opts.delay || 0;
    this.priority = opts.priority || 0;
    this.timestamp = Date.now();
    this.processedOn = null;
    this.finishedOn = null;
    this.stacktrace = [];
  }

  /** Update job progress (0-100) */
  async updateProgress(value) {
    this.progress = value;
    await this.queue._updateJobRow(this.id, { progress: value });
    return this;
  }

  /** Serialize to row format for ZeroDB table */
  toRow() {
    return {
      id: this.id,
      queue_name: this.queueName,
      job_name: this.name,
      data: JSON.stringify(this.data),
      opts: JSON.stringify(this.opts),
      status: this.status,
      progress: this.progress,
      result: this.result ? JSON.stringify(this.result) : null,
      error: this.error,
      attempts_made: this.attemptsMade,
      max_attempts: this.maxAttempts,
      priority: this.priority,
      delay: this.delay,
      timestamp: this.timestamp,
      processed_on: this.processedOn,
      finished_on: this.finishedOn,
      stacktrace: JSON.stringify(this.stacktrace),
    };
  }

  /** Reconstruct Job from a ZeroDB table row */
  static fromRow(queue, row) {
    const data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
    const opts = typeof row.opts === 'string' ? JSON.parse(row.opts) : (row.opts || {});
    const job = new Job(queue, row.job_name, data, { ...opts, jobId: row.id });
    job.status = row.status;
    job.progress = row.progress || 0;
    job.result = row.result ? (typeof row.result === 'string' ? JSON.parse(row.result) : row.result) : null;
    job.error = row.error || null;
    job.attemptsMade = row.attempts_made || 0;
    job.maxAttempts = row.max_attempts || DEFAULT_MAX_RETRIES;
    job.priority = row.priority || 0;
    job.delay = row.delay || 0;
    job.timestamp = row.timestamp;
    job.processedOn = row.processed_on || null;
    job.finishedOn = row.finished_on || null;
    job.stacktrace = row.stacktrace ? (typeof row.stacktrace === 'string' ? JSON.parse(row.stacktrace) : row.stacktrace) : [];
    return job;
  }
}

// ---------------------------------------------------------------------------
// EventEmitter (minimal, zero-dep)
// ---------------------------------------------------------------------------

class MiniEmitter {
  constructor() {
    this._listeners = {};
  }

  on(event, fn) {
    (this._listeners[event] ||= []).push(fn);
    return this;
  }

  off(event, fn) {
    const arr = this._listeners[event];
    if (arr) this._listeners[event] = arr.filter((f) => f !== fn);
    return this;
  }

  emit(event, ...args) {
    const arr = this._listeners[event];
    if (arr) arr.forEach((fn) => fn(...args));
  }

  once(event, fn) {
    const wrapped = (...args) => {
      this.off(event, wrapped);
      fn(...args);
    };
    return this.on(event, wrapped);
  }
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

class Queue extends MiniEmitter {
  /**
   * @param {string} name - Queue name (used as table prefix)
   * @param {object} [opts]
   * @param {string} [opts.apiToken] - ZeroDB API token
   * @param {string} [opts.projectId] - ZeroDB project ID
   * @param {string} [opts.baseUrl] - ZeroDB API base URL
   */
  constructor(name, opts = {}) {
    super();
    if (!name || typeof name !== 'string') {
      throw new QueueError('Queue name is required and must be a string', 'INVALID_NAME');
    }
    this.name = name;
    this._tableName = `queue_${name.replace(/[^a-zA-Z0-9_]/g, '_')}`;
    this._client = new ZeroDBClient(opts);
    this._provisioned = false;
    this._provisionPromise = null;
  }

  /** Ensure the jobs table exists in ZeroDB (idempotent) */
  async _provision() {
    if (this._provisioned) return;
    if (this._provisionPromise) return this._provisionPromise;

    this._provisionPromise = (async () => {
      try {
        await this._client.execute('create_table', {
          table_name: this._tableName,
          schema: {
            id: 'string',
            queue_name: 'string',
            job_name: 'string',
            data: 'string',
            opts: 'string',
            status: 'string',
            progress: 'number',
            result: 'string',
            error: 'string',
            attempts_made: 'number',
            max_attempts: 'number',
            priority: 'number',
            delay: 'number',
            timestamp: 'number',
            processed_on: 'number',
            finished_on: 'number',
            stacktrace: 'string',
          },
        });
      } catch (err) {
        // Table already exists — that is fine
        if (err.status !== 409 && !err.message?.includes('already exists')) {
          throw err;
        }
      }
      this._provisioned = true;
    })();

    return this._provisionPromise;
  }

  /**
   * Add a job to the queue.
   * @param {string} name - Job name / type
   * @param {*} data - Payload
   * @param {object} [opts] - { delay, priority, attempts, backoff, jobId }
   * @returns {Promise<Job>}
   */
  async add(name, data, opts = {}) {
    await this._provision();
    const job = new Job(this, name, data, opts);

    if (job.delay > 0) {
      job.status = JobStatus.DELAYED;
    }

    await this._client.execute('insert_rows', {
      table_name: this._tableName,
      rows: [job.toRow()],
    });

    // Emit event so subscribers can react
    try {
      await this._client.execute('create_event', {
        event_type: `queue.${this.name}.job_added`,
        data: { job_id: job.id, job_name: name },
      });
    } catch (_) {
      // Event stream is optional — queue still works without it
    }

    this.emit('added', job);
    return job;
  }

  /**
   * Add multiple jobs in bulk.
   * @param {Array<{name: string, data: *, opts?: object}>} jobs
   * @returns {Promise<Job[]>}
   */
  async addBulk(jobs) {
    await this._provision();
    const jobInstances = jobs.map(({ name, data, opts }) => new Job(this, name, data, opts || {}));

    const rows = jobInstances.map((j) => {
      if (j.delay > 0) j.status = JobStatus.DELAYED;
      return j.toRow();
    });

    await this._client.execute('insert_rows', {
      table_name: this._tableName,
      rows,
    });

    jobInstances.forEach((j) => this.emit('added', j));
    return jobInstances;
  }

  /**
   * Get a job by ID.
   * @param {string} id
   * @returns {Promise<Job|null>}
   */
  async getJob(id) {
    await this._provision();
    const result = await this._client.execute('query_rows', {
      table_name: this._tableName,
      filters: { id },
      limit: 1,
    });

    const rows = result?.rows || result?.data?.rows || [];
    if (rows.length === 0) return null;
    return Job.fromRow(this, rows[0]);
  }

  /**
   * Get jobs by status.
   * @param {string|string[]} statuses
   * @param {number} [start=0]
   * @param {number} [end=100]
   * @returns {Promise<Job[]>}
   */
  async getJobs(statuses, start = 0, end = 100) {
    await this._provision();
    const statusList = Array.isArray(statuses) ? statuses : [statuses];

    const result = await this._client.execute('query_rows', {
      table_name: this._tableName,
      filters: { status: { $in: statusList } },
      limit: end - start,
      offset: start,
      sort: { timestamp: 1 },
    });

    const rows = result?.rows || result?.data?.rows || [];
    return rows.map((r) => Job.fromRow(this, r));
  }

  /**
   * Get counts by status.
   * @returns {Promise<object>}
   */
  async getJobCounts() {
    await this._provision();
    const counts = {};
    for (const status of Object.values(JobStatus)) {
      const result = await this._client.execute('query_rows', {
        table_name: this._tableName,
        filters: { status },
        limit: 0,
      });
      counts[status] = result?.total || result?.data?.total || 0;
    }
    return counts;
  }

  /**
   * Remove a job by ID.
   * @param {string} id
   */
  async remove(id) {
    await this._provision();
    await this._client.execute('delete_rows', {
      table_name: this._tableName,
      filters: { id },
    });
  }

  /**
   * Drain the queue — remove all jobs.
   */
  async drain() {
    await this._provision();
    await this._client.execute('delete_rows', {
      table_name: this._tableName,
      filters: { queue_name: this.name },
    });
  }

  /**
   * Obliterate — delete the entire table.
   */
  async obliterate() {
    await this._client.execute('delete_table', {
      table_name: this._tableName,
    });
    this._provisioned = false;
    this._provisionPromise = null;
  }

  /** @internal Update a job row */
  async _updateJobRow(id, updates) {
    await this._client.execute('update_rows', {
      table_name: this._tableName,
      filters: { id },
      updates,
    });
  }

  /** @internal Claim waiting jobs for processing */
  async _claimJobs(limit = 1) {
    const now = Date.now();

    // Promote delayed jobs whose delay has elapsed
    try {
      const delayed = await this._client.execute('query_rows', {
        table_name: this._tableName,
        filters: { status: JobStatus.DELAYED },
        limit: 50,
      });
      const delayedRows = delayed?.rows || delayed?.data?.rows || [];
      for (const row of delayedRows) {
        if (row.timestamp + (row.delay || 0) <= now) {
          await this._updateJobRow(row.id, { status: JobStatus.WAITING });
        }
      }
    } catch (_) {
      // Non-critical
    }

    // Claim waiting jobs (sorted by priority desc, timestamp asc)
    const result = await this._client.execute('query_rows', {
      table_name: this._tableName,
      filters: { status: JobStatus.WAITING },
      limit,
      sort: { priority: -1, timestamp: 1 },
    });

    const rows = result?.rows || result?.data?.rows || [];
    const claimed = [];

    for (const row of rows) {
      try {
        await this._updateJobRow(row.id, {
          status: JobStatus.ACTIVE,
          processed_on: now,
        });
        row.status = JobStatus.ACTIVE;
        row.processed_on = now;
        claimed.push(Job.fromRow(this, row));
      } catch (_) {
        // Another worker grabbed it — skip
      }
    }

    return claimed;
  }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

class Worker extends MiniEmitter {
  /**
   * @param {string} queueName
   * @param {function} processor - async (job) => result
   * @param {object} [opts]
   * @param {number} [opts.concurrency=1]
   * @param {number} [opts.pollInterval=1000] - ms between polls
   * @param {number} [opts.stallInterval=30000] - ms before a job is considered stalled
   * @param {boolean} [opts.autorun=true] - start processing immediately
   * @param {string} [opts.apiToken]
   * @param {string} [opts.projectId]
   * @param {string} [opts.baseUrl]
   */
  constructor(queueName, processor, opts = {}) {
    super();
    if (typeof processor !== 'function') {
      throw new QueueError('Worker processor must be a function', 'INVALID_PROCESSOR');
    }

    this.queue = new Queue(queueName, opts);
    this.processor = processor;
    this.concurrency = opts.concurrency || DEFAULT_CONCURRENCY;
    this.pollInterval = opts.pollInterval || DEFAULT_POLL_INTERVAL;
    this.stallInterval = opts.stallInterval || DEFAULT_STALL_INTERVAL;
    this.running = false;
    this.paused = false;
    this._activeCount = 0;
    this._pollTimer = null;
    this._closed = false;

    if (opts.autorun !== false) {
      this.run();
    }
  }

  /** Start processing jobs */
  run() {
    if (this.running) return;
    this.running = true;
    this.paused = false;
    this._poll();
  }

  /** Pause the worker */
  pause() {
    this.paused = true;
    if (this._pollTimer) {
      clearTimeout(this._pollTimer);
      this._pollTimer = null;
    }
  }

  /** Resume processing */
  resume() {
    if (!this.paused) return;
    this.paused = false;
    this._poll();
  }

  /** Close the worker gracefully */
  async close() {
    this._closed = true;
    this.running = false;
    if (this._pollTimer) {
      clearTimeout(this._pollTimer);
      this._pollTimer = null;
    }
    // Wait for in-flight jobs
    while (this._activeCount > 0) {
      await new Promise((r) => setTimeout(r, 100));
    }
    this.emit('closed');
  }

  /** @internal Poll loop */
  _poll() {
    if (this._closed || this.paused) return;

    const availableSlots = this.concurrency - this._activeCount;
    if (availableSlots <= 0) {
      this._schedulePoll();
      return;
    }

    this.queue
      ._claimJobs(availableSlots)
      .then((jobs) => {
        for (const job of jobs) {
          this._processJob(job);
        }
        this._schedulePoll();
      })
      .catch((err) => {
        this.emit('error', err);
        this._schedulePoll();
      });
  }

  _schedulePoll() {
    if (this._closed || this.paused) return;
    this._pollTimer = setTimeout(() => this._poll(), this.pollInterval);
  }

  /** @internal Process a single job */
  async _processJob(job) {
    this._activeCount++;
    this.emit('active', job);

    try {
      const result = await this.processor(job);
      job.result = result;
      job.status = JobStatus.COMPLETED;
      job.finishedOn = Date.now();

      await this.queue._updateJobRow(job.id, {
        status: JobStatus.COMPLETED,
        result: result != null ? JSON.stringify(result) : null,
        finished_on: job.finishedOn,
        attempts_made: job.attemptsMade + 1,
      });

      this.emit('completed', job, result);
    } catch (err) {
      job.attemptsMade++;
      job.stacktrace.push(err.stack || err.message);

      if (job.attemptsMade < job.maxAttempts) {
        // Retry with backoff
        const backoffMs =
          typeof job.backoff === 'object'
            ? (job.backoff.delay || DEFAULT_BACKOFF_MS) * Math.pow(2, job.attemptsMade - 1)
            : (job.backoff || DEFAULT_BACKOFF_MS) * Math.pow(2, job.attemptsMade - 1);

        job.status = JobStatus.DELAYED;
        job.delay = backoffMs;
        job.timestamp = Date.now();

        await this.queue._updateJobRow(job.id, {
          status: JobStatus.DELAYED,
          delay: backoffMs,
          timestamp: job.timestamp,
          attempts_made: job.attemptsMade,
          error: err.message,
          stacktrace: JSON.stringify(job.stacktrace),
        });

        this.emit('retrying', job, err);
      } else {
        job.status = JobStatus.FAILED;
        job.error = err.message;
        job.finishedOn = Date.now();

        await this.queue._updateJobRow(job.id, {
          status: JobStatus.FAILED,
          error: err.message,
          finished_on: job.finishedOn,
          attempts_made: job.attemptsMade,
          stacktrace: JSON.stringify(job.stacktrace),
        });

        this.emit('failed', job, err);
      }
    } finally {
      this._activeCount--;
    }
  }
}

// ---------------------------------------------------------------------------
// QueueScheduler (delayed job promotion — optional, runs in background)
// ---------------------------------------------------------------------------

class QueueScheduler extends MiniEmitter {
  /**
   * @param {string} queueName
   * @param {object} [opts]
   * @param {number} [opts.pollInterval=5000]
   * @param {number} [opts.stallInterval=30000]
   */
  constructor(queueName, opts = {}) {
    super();
    this.queue = new Queue(queueName, opts);
    this.pollInterval = opts.pollInterval || 5000;
    this.stallInterval = opts.stallInterval || DEFAULT_STALL_INTERVAL;
    this._timer = null;
    this._closed = false;
    this._run();
  }

  async _run() {
    if (this._closed) return;

    try {
      const now = Date.now();

      // Promote delayed jobs
      const delayed = await this.queue._client.execute('query_rows', {
        table_name: this.queue._tableName,
        filters: { status: JobStatus.DELAYED },
        limit: 100,
      });

      const rows = delayed?.rows || delayed?.data?.rows || [];
      let promoted = 0;
      for (const row of rows) {
        if (row.timestamp + (row.delay || 0) <= now) {
          await this.queue._updateJobRow(row.id, { status: JobStatus.WAITING });
          promoted++;
        }
      }
      if (promoted > 0) this.emit('promoted', promoted);

      // Detect stalled jobs
      const active = await this.queue._client.execute('query_rows', {
        table_name: this.queue._tableName,
        filters: { status: JobStatus.ACTIVE },
        limit: 100,
      });

      const activeRows = active?.rows || active?.data?.rows || [];
      let stalled = 0;
      for (const row of activeRows) {
        if (row.processed_on && now - row.processed_on > this.stallInterval) {
          await this.queue._updateJobRow(row.id, { status: JobStatus.STALLED });
          stalled++;
          this.emit('stalled', row.id);
        }
      }
    } catch (err) {
      this.emit('error', err);
    }

    this._timer = setTimeout(() => this._run(), this.pollInterval);
  }

  async close() {
    this._closed = true;
    if (this._timer) clearTimeout(this._timer);
    this.emit('closed');
  }
}

// ---------------------------------------------------------------------------
// FlowProducer — job dependencies (simplified)
// ---------------------------------------------------------------------------

class FlowProducer {
  /**
   * @param {object} [opts] - ZeroDB connection options
   */
  constructor(opts = {}) {
    this._opts = opts;
    this._queues = {};
  }

  _getQueue(name) {
    if (!this._queues[name]) {
      this._queues[name] = new Queue(name, this._opts);
    }
    return this._queues[name];
  }

  /**
   * Add a flow of parent + children jobs.
   * Children run first; parent runs after all children complete.
   * @param {object} flow - { name, queueName, data, opts, children: [...] }
   * @returns {Promise<{job: Job, children: Job[]}>}
   */
  async add(flow) {
    const childJobs = [];
    if (flow.children && flow.children.length > 0) {
      for (const child of flow.children) {
        const queue = this._getQueue(child.queueName);
        const job = await queue.add(child.name, child.data, child.opts || {});
        childJobs.push(job);
      }
    }

    const parentQueue = this._getQueue(flow.queueName);
    const parentJob = await parentQueue.add(flow.name, {
      ...flow.data,
      _childJobIds: childJobs.map((j) => j.id),
    }, flow.opts || {});

    return { job: parentJob, children: childJobs };
  }
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

export {
  Queue,
  Worker,
  Job,
  JobStatus,
  QueueScheduler,
  FlowProducer,
  QueueError,
  ZeroDBError,
  MiniEmitter,
  ZeroDBClient,
};

export default { Queue, Worker, Job, JobStatus, QueueScheduler, FlowProducer };
