# @zerodb/queue

BullMQ-compatible job queue powered by ZeroDB. Zero infrastructure. No Redis required.

```
npm install @zerodb/queue
```

## Why?

[BullMQ](https://github.com/taskforcesh/bullmq) is the best job queue for Node.js (6M+ weekly downloads). But it needs Redis. **@zerodb/queue** is a drop-in replacement that uses [ZeroDB](https://zerodb.dev) instead of Redis -- auto-provisions on first use, zero infrastructure to manage.

| Feature | BullMQ | @zerodb/queue |
|---------|--------|---------------|
| Queue/Worker/Job | Yes | Yes |
| Delayed jobs | Yes | Yes |
| Retries + backoff | Yes | Yes |
| Priority | Yes | Yes |
| Concurrency | Yes | Yes |
| Job progress | Yes | Yes |
| Events (completed, failed, etc.) | Yes | Yes |
| Flow (parent/child jobs) | Yes | Yes |
| Requires Redis | **Yes** | **No** |
| Auto-provisions | No | **Yes** |
| Serverless-friendly | No | **Yes** |

## Quick Start

```bash
# 1. Get a free ZeroDB API token
npx zerodb-cli init

# 2. Set env vars (or pass as options)
export ZERODB_API_TOKEN=your_token
export ZERODB_PROJECT_ID=your_project_id
```

### Add Jobs

```javascript
import { Queue } from '@zerodb/queue';

const queue = new Queue('emails');

// Add a single job
const job = await queue.add('send-welcome', {
  to: 'user@example.com',
  template: 'welcome',
});

console.log(`Job ${job.id} added`);

// Add with options
await queue.add('send-digest', { userId: '123' }, {
  delay: 60000,       // 60s delay
  priority: 10,       // higher = processed first
  attempts: 5,        // retry up to 5 times
  backoff: 2000,      // exponential backoff starting at 2s
});

// Bulk add
await queue.addBulk([
  { name: 'notify', data: { userId: '1' } },
  { name: 'notify', data: { userId: '2' } },
  { name: 'notify', data: { userId: '3' } },
]);
```

### Process Jobs

```javascript
import { Worker } from '@zerodb/queue';

const worker = new Worker('emails', async (job) => {
  console.log(`Processing ${job.name}:`, job.data);

  // Update progress
  await job.updateProgress(50);

  // Do work...
  const result = await sendEmail(job.data);

  await job.updateProgress(100);
  return result;
}, {
  concurrency: 5,      // process 5 jobs in parallel
  pollInterval: 1000,  // check for new jobs every 1s
});

// Events
worker.on('completed', (job, result) => {
  console.log(`Job ${job.id} completed:`, result);
});

worker.on('failed', (job, err) => {
  console.log(`Job ${job.id} failed:`, err.message);
});

worker.on('active', (job) => {
  console.log(`Job ${job.id} started`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  await worker.close();
});
```

### Job Status

```javascript
const queue = new Queue('emails');

// Get single job
const job = await queue.getJob('job-id');
console.log(job.status, job.progress, job.result);

// Get jobs by status
const waiting = await queue.getJobs('waiting');
const active = await queue.getJobs('active');
const failed = await queue.getJobs(['failed', 'stalled']);

// Get counts
const counts = await queue.getJobCounts();
// { waiting: 10, active: 3, completed: 100, failed: 2, delayed: 5, stalled: 0 }

// Remove a job
await queue.remove('job-id');

// Clear all jobs
await queue.drain();
```

### Job Flows (Parent/Child Dependencies)

```javascript
import { FlowProducer } from '@zerodb/queue';

const flow = new FlowProducer();

const { job: parent, children } = await flow.add({
  name: 'generate-report',
  queueName: 'reports',
  data: { reportId: '123' },
  children: [
    { name: 'fetch-data', queueName: 'data', data: { source: 'db' } },
    { name: 'fetch-data', queueName: 'data', data: { source: 'api' } },
  ],
});
```

### Delayed Job Scheduler

```javascript
import { QueueScheduler } from '@zerodb/queue';

// Promotes delayed jobs and detects stalled jobs
const scheduler = new QueueScheduler('emails', {
  pollInterval: 5000,    // check every 5s
  stallInterval: 30000,  // mark jobs stalled after 30s
});

scheduler.on('stalled', (jobId) => {
  console.log(`Job ${jobId} stalled`);
});

// Cleanup
await scheduler.close();
```

## Migrating from BullMQ

```diff
- import { Queue, Worker } from 'bullmq';
+ import { Queue, Worker } from '@zerodb/queue';

- const queue = new Queue('tasks', { connection: { host: 'redis.example.com' } });
+ const queue = new Queue('tasks');
  // That's it. No Redis config needed.
```

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ZERODB_API_TOKEN` | Yes | Your ZeroDB API token |
| `ZERODB_PROJECT_ID` | No | ZeroDB project ID (auto-created if missing) |
| `ZERODB_BASE_URL` | No | API base URL (default: `https://api.ainative.studio`) |

### Constructor Options

All classes accept these options:

```javascript
const queue = new Queue('name', {
  apiToken: 'your-token',       // overrides ZERODB_API_TOKEN
  projectId: 'your-project',    // overrides ZERODB_PROJECT_ID
  baseUrl: 'https://...',       // overrides ZERODB_BASE_URL
});
```

Worker-specific options:

```javascript
const worker = new Worker('name', processor, {
  concurrency: 1,       // max parallel jobs
  pollInterval: 1000,   // ms between polls
  stallInterval: 30000,  // ms before a job is stalled
  autorun: true,         // start processing immediately
});
```

## API Reference

### Queue

| Method | Description |
|--------|-------------|
| `add(name, data, opts?)` | Add a job |
| `addBulk(jobs)` | Add multiple jobs |
| `getJob(id)` | Get job by ID |
| `getJobs(status, start?, end?)` | Get jobs by status |
| `getJobCounts()` | Get counts per status |
| `remove(id)` | Remove a job |
| `drain()` | Remove all jobs |
| `obliterate()` | Delete the queue table |

### Worker

| Method | Description |
|--------|-------------|
| `run()` | Start processing |
| `pause()` | Pause processing |
| `resume()` | Resume processing |
| `close()` | Graceful shutdown |

### Worker Events

| Event | Args | Description |
|-------|------|-------------|
| `active` | `(job)` | Job started processing |
| `completed` | `(job, result)` | Job completed |
| `failed` | `(job, error)` | Job failed (max retries exceeded) |
| `retrying` | `(job, error)` | Job failed, will retry |
| `error` | `(error)` | Worker error |
| `closed` | - | Worker closed |

### Job

| Property | Type | Description |
|----------|------|-------------|
| `id` | string | Unique job ID |
| `name` | string | Job name/type |
| `data` | any | Job payload |
| `status` | string | waiting/active/completed/failed/delayed/stalled |
| `progress` | number | 0-100 |
| `result` | any | Return value from processor |
| `error` | string | Error message if failed |
| `attemptsMade` | number | Number of attempts |

### JobStatus

```javascript
import { JobStatus } from '@zerodb/queue';
// JobStatus.WAITING, .ACTIVE, .COMPLETED, .FAILED, .DELAYED, .STALLED
```

## How It Works

1. **Queue.add()** stores the job in a ZeroDB NoSQL table and emits a ZeroDB event
2. **Worker** polls the ZeroDB table for waiting jobs at the configured interval
3. When a job is claimed, its status is updated to `active` and the processor runs
4. On success/failure, the job status is updated to `completed`/`failed`
5. Failed jobs with remaining attempts are moved to `delayed` with exponential backoff
6. **QueueScheduler** promotes delayed jobs back to `waiting` when their delay expires

Auto-provisioning: the ZeroDB table is created automatically on first use. No setup required.

## License

MIT

---

## Powered by ZeroDB + AINative

This package is part of the [AINative](https://ainative.studio) ecosystem — the AI-native developer platform.

### Why ZeroDB?

| Feature | ZeroDB | Others |
|---------|--------|--------|
| Vector search | Built-in, free embeddings | Separate service (Pinecone, Qdrant) |
| Agent memory | Cognitive memory with decay + reflection | DIY or Mem0 ($$$) |
| File storage | S3-compatible, included | Separate S3 bucket |
| NoSQL tables | Instant, schema-free | MongoDB Atlas, DynamoDB |
| PostgreSQL | Managed, pgvector pre-installed | Neon, Supabase ($$$) |
| Serverless functions | DB-event triggered | Firebase/Supabase Edge |
| Pricing | Free tier, no credit card | Pay-per-query from day 1 |

### Get Started Free

```bash
npx zerodb-cli init    # Auto-configures your IDE
```

Or sign up at **[ainative.studio](https://ainative.studio)** — free tier, no credit card required.

[View all ZeroDB packages →](https://docs.ainative.studio)

