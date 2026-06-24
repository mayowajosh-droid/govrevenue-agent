import { Queue, Worker } from "bullmq";
import { runFullIngest, runSourceIngest } from "./orchestrator.js";
import { DATA_SOURCES } from "./source-registry.js";

const QUEUE_NAME = "atlasrevenue-ingest";

// Cadence → repeat interval in ms
const CADENCE_MS: Record<string, number> = {
  realtime: 15 * 60 * 1000,       // every 15 min
  daily:    24 * 60 * 60 * 1000,  // every 24h
  weekly:    7 * 24 * 60 * 60 * 1000,
  monthly:  30 * 24 * 60 * 60 * 1000,
};

let ingestQueue: Queue | null = null;

export function startIngestScheduler(redisConnection: object): void {
  ingestQueue = new Queue(QUEUE_NAME, { connection: redisConnection as any });

  // Schedule one repeating job per cadence bucket (not per source — avoids 43 separate jobs)
  const cadences = ["realtime", "daily", "weekly", "monthly"] as const;
  for (const cadence of cadences) {
    const sources = DATA_SOURCES.filter(s => s.cadence === cadence && s.live);
    if (sources.length === 0) continue;

    ingestQueue.add(
      `ingest-${cadence}`,
      { cadence, sourceIds: sources.map(s => s.id) },
      {
        repeat: { every: CADENCE_MS[cadence] },
        jobId: `ingest-cadence-${cadence}`,
        removeOnComplete: { count: 5 },
        removeOnFail: { age: 60 * 60 * 24 * 3 },
      }
    ).catch(err => console.error(`[ingest-scheduler] failed to schedule ${cadence} job:`, err));
  }

  // Also schedule a full-ingest daily sweep as a safety net
  ingestQueue.add(
    "ingest-full-sweep",
    { cadence: "full" },
    {
      repeat: { every: 24 * 60 * 60 * 1000 },
      jobId: "ingest-full-daily",
      removeOnComplete: { count: 3 },
      removeOnFail: { age: 60 * 60 * 24 * 3 },
    }
  ).catch(err => console.error("[ingest-scheduler] failed to schedule full sweep:", err));

  // Worker processes jobs
  const worker = new Worker(
    QUEUE_NAME,
    async (job) => {
      const { cadence, sourceIds } = job.data as { cadence: string; sourceIds?: string[] };
      console.log(`[ingest] Running ${cadence} ingest (${sourceIds?.length ?? "all"} sources)`);

      if (cadence === "full" || !sourceIds) {
        await runFullIngest();
        return;
      }

      // Run each source in the cadence bucket sequentially to respect rate limits
      for (const id of sourceIds) {
        const result = await runSourceIngest(id);
        if (result.error) {
          console.warn(`[ingest] ${id}: ${result.error}`);
        } else {
          console.log(`[ingest] ${id}: ${result.recordsIngested} records (${result.durationMs}ms)`);
        }
      }
    },
    {
      connection: redisConnection as any,
      concurrency: 1,
      lockDuration: 30 * 60 * 1000,   // 30 min — some sources are slow
      stalledInterval: 5 * 60 * 1000,
    }
  );

  worker.on("completed", (job) => {
    console.log(`[ingest] job ${job.name} completed`);
  });
  worker.on("failed", (job, err) => {
    console.error(`[ingest] job ${job?.name} failed:`, err?.message);
  });

  console.log("[ingest-scheduler] started — realtime/daily/weekly/monthly cadences scheduled");

  // Run an immediate daily pass on startup to warm up canonical_ingest
  ingestQueue.add(
    "ingest-startup",
    { cadence: "daily", sourceIds: DATA_SOURCES.filter(s => s.cadence === "daily" && s.live).map(s => s.id) },
    { removeOnComplete: true, removeOnFail: { age: 60 * 60 * 24 } }
  ).catch(() => {});
}

export function getIngestQueue(): Queue | null {
  return ingestQueue;
}
