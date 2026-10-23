import type { Database } from "bun:sqlite";
import { Effect } from "effect";
import {
  sealWorkerUidWorkspacesSync,
  stopWorkerUidProcessesSync,
  workerUidRange,
} from "../worker-security.js";
import { SqliteClient, tryDb } from "./sqlite-client.js";

/** Synchronous transactions make allocation atomic across gateway connections. */
export const makeWorkerIdentityRepo = (db: Database) => {
  const acquire = (sessionId: string) =>
    tryDb(
      () =>
        db
          .transaction(() => {
            const range = workerUidRange(process.env);
            if (range === undefined) return undefined;
            const previous = db
              .query<{ uid: number; active: number }, [string]>(
                "SELECT uid, active FROM worker_identity WHERE session_id=?",
              )
              .get(sessionId);
            const occupied = new Set(
              db
                .query<{ uid: number }, [string]>(
                  "SELECT uid FROM worker_identity WHERE active=1 AND session_id<>?",
                )
                .all(sessionId)
                .map((row) => row.uid),
            );
            let uid = previous?.uid;
            if (uid !== undefined) {
              if (uid < range.base || uid >= range.base + range.count)
                throw new Error(
                  "Persisted worker uid is outside the configured range",
                );
              if (occupied.has(uid))
                throw new Error(
                  `Worker uid ${uid} for resume is busy; retry when its current session ends`,
                );
            } else {
              for (
                let candidate = range.base;
                candidate < range.base + range.count;
                candidate++
              ) {
                if (!occupied.has(candidate)) {
                  uid = candidate;
                  break;
                }
              }
            }
            if (uid === undefined)
              throw new Error(
                "Worker uid range exhausted; retry when a session ends or increase WORKER_UID_COUNT",
              );
            db.query(
              "INSERT INTO worker_identity(session_id, uid, active) VALUES (?, ?, 1) ON CONFLICT(session_id) DO UPDATE SET active=1",
            ).run(sessionId, uid);
            return String(uid);
          })
          .immediate(),
      "WorkerIdentityRepo.acquire",
    );
  // Called after worker teardown. Revoke escaped processes and all retained roots before freeing the slot.
  const release = (sessionId: string, workspaceRoot: string) =>
    tryDb(
      () =>
        db
          .transaction(() => {
            const row = db
              .query<{ uid: number; active: number; state: string }, [string]>(
                "SELECT i.uid, i.active, r.state FROM worker_identity i JOIN agent_run r ON r.session_id=i.session_id WHERE i.session_id=?",
              )
              .get(sessionId);
            if (
              row === null ||
              row.active === 0 ||
              !["succeeded", "failed", "canceled"].includes(row.state)
            )
              return;
            stopWorkerUidProcessesSync(row.uid);
            sealWorkerUidWorkspacesSync(workspaceRoot, row.uid);
            db.query(
              "UPDATE worker_identity SET active=0 WHERE session_id=?",
            ).run(sessionId);
          })
          .immediate(),
      "WorkerIdentityRepo.release",
    );
  return { acquire, release };
};

export class WorkerIdentityRepo extends Effect.Service<WorkerIdentityRepo>()(
  "WorkerIdentityRepo",
  {
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;
      return makeWorkerIdentityRepo(db);
    }),
  },
) {}
