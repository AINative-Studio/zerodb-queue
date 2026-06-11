#!/usr/bin/env node

/**
 * Zero-dep build script: copies ESM source and generates CJS + .d.ts
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, '..');
const dist = join(root, 'dist');

if (!existsSync(dist)) mkdirSync(dist, { recursive: true });

// Read ESM source
const esm = readFileSync(join(root, 'src', 'index.js'), 'utf8');

// Write ESM
writeFileSync(join(dist, 'index.js'), esm);

// Generate CJS wrapper
const cjs = esm
  .replace(/^export\s+\{[^}]*\};?\s*$/m, '')
  .replace(/^export\s+default\s+.*$/m, '')
  .replace(/^import\s+/gm, '// import ')
  + `
module.exports = { Queue, Worker, Job, JobStatus, QueueScheduler, FlowProducer, QueueError, ZeroDBError, MiniEmitter, ZeroDBClient };
module.exports.default = { Queue, Worker, Job, JobStatus, QueueScheduler, FlowProducer };
`;

writeFileSync(join(dist, 'index.cjs'), cjs);

// Generate TypeScript declarations
const dts = `/**
 * @zerodb/queue — BullMQ-compatible job queue powered by ZeroDB
 */

export interface QueueOptions {
  apiToken?: string;
  projectId?: string;
  baseUrl?: string;
}

export interface JobOptions {
  jobId?: string;
  delay?: number;
  priority?: number;
  attempts?: number;
  backoff?: number | { type: string; delay: number };
}

export interface WorkerOptions extends QueueOptions {
  concurrency?: number;
  pollInterval?: number;
  stallInterval?: number;
  autorun?: boolean;
}

export interface QueueSchedulerOptions extends QueueOptions {
  pollInterval?: number;
  stallInterval?: number;
}

export declare const JobStatus: {
  readonly WAITING: 'waiting';
  readonly ACTIVE: 'active';
  readonly COMPLETED: 'completed';
  readonly FAILED: 'failed';
  readonly DELAYED: 'delayed';
  readonly STALLED: 'stalled';
};

export type JobStatusType = typeof JobStatus[keyof typeof JobStatus];

export declare class QueueError extends Error {
  code: string;
  constructor(message: string, code: string);
}

export declare class ZeroDBError extends QueueError {
  status: number;
  constructor(message: string, status: number);
}

export declare class MiniEmitter {
  on(event: string, fn: (...args: any[]) => void): this;
  off(event: string, fn: (...args: any[]) => void): this;
  once(event: string, fn: (...args: any[]) => void): this;
  emit(event: string, ...args: any[]): void;
}

export declare class Job<T = any, R = any> {
  readonly id: string;
  readonly queueName: string;
  readonly name: string;
  data: T;
  opts: JobOptions;
  status: JobStatusType;
  progress: number;
  result: R | null;
  error: string | null;
  attemptsMade: number;
  maxAttempts: number;
  timestamp: number;
  processedOn: number | null;
  finishedOn: number | null;
  stacktrace: string[];

  updateProgress(value: number): Promise<this>;
  toRow(): Record<string, any>;
  static fromRow<T, R>(queue: Queue, row: Record<string, any>): Job<T, R>;
}

export declare class Queue<T = any> extends MiniEmitter {
  readonly name: string;

  constructor(name: string, opts?: QueueOptions);

  add(name: string, data: T, opts?: JobOptions): Promise<Job<T>>;
  addBulk(jobs: Array<{ name: string; data: T; opts?: JobOptions }>): Promise<Job<T>[]>;
  getJob(id: string): Promise<Job<T> | null>;
  getJobs(statuses: JobStatusType | JobStatusType[], start?: number, end?: number): Promise<Job<T>[]>;
  getJobCounts(): Promise<Record<JobStatusType, number>>;
  remove(id: string): Promise<void>;
  drain(): Promise<void>;
  obliterate(): Promise<void>;
}

export declare class Worker<T = any, R = any> extends MiniEmitter {
  readonly queue: Queue<T>;
  readonly concurrency: number;
  running: boolean;
  paused: boolean;

  constructor(queueName: string, processor: (job: Job<T>) => Promise<R>, opts?: WorkerOptions);

  run(): void;
  pause(): void;
  resume(): void;
  close(): Promise<void>;
}

export declare class QueueScheduler extends MiniEmitter {
  constructor(queueName: string, opts?: QueueSchedulerOptions);
  close(): Promise<void>;
}

export interface FlowJob {
  name: string;
  queueName: string;
  data: any;
  opts?: JobOptions;
  children?: FlowJob[];
}

export declare class FlowProducer {
  constructor(opts?: QueueOptions);
  add(flow: FlowJob): Promise<{ job: Job; children: Job[] }>;
}

export declare class ZeroDBClient {
  constructor(opts?: QueueOptions);
  execute(operation: string, params?: Record<string, any>): Promise<any>;
}

declare const _default: {
  Queue: typeof Queue;
  Worker: typeof Worker;
  Job: typeof Job;
  JobStatus: typeof JobStatus;
  QueueScheduler: typeof QueueScheduler;
  FlowProducer: typeof FlowProducer;
};

export default _default;
`;

writeFileSync(join(dist, 'index.d.ts'), dts);

console.log('Built: dist/index.js (ESM), dist/index.cjs (CJS), dist/index.d.ts (types)');
