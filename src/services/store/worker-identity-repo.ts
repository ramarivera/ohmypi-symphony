import type { Database } from "bun:sqlite";
import { Context, Effect, Layer } from "effect";
import {
  sealWorkerUidWorkspacesSync,
  stopWorkerUidProcessesSync,
  workerUidRange,
} from "../worker-security.js";
import { cleanupWorkerSharedFilesSync } from "../workspace-process.js";
import { SqliteClient, tryDb } from "./sqlite-client.js";

/** Called inside the caller's immediate transaction. Failures reserve only this uid. */
export const retireWorkerIdentitySync = (
  db: Database,
  sessionId: string,
  uid: number,
  workspaceRoot: string,
): boolean => {
  try {
    stopWorkerUidProcessesSync(uid);
    cleanupWorkerSharedFilesSync(uid);
    sealWorkerUidWorkspacesSync(workspaceRoot, uid);
  } catch {
    const now = Date.now();
    db.query(
      `INSERT INTO run_event(source_key, session_id, kind, level, text, payload_json, created_at, updated_at)
       VALUES (?, ?, 'worker_uid_quarantined', 'warn', ?, ?, ?, ?)
       ON CONFLICT(source_key) DO UPDATE SET updated_at=excluded.updated_at`,
    ).run(
      `worker-uid-quarantined:${sessionId}`,
      sessionId,
      "Worker uid retirement failed; identity remains reserved. Allocation and retention will retry.",
      JSON.stringify({ uid }),
      now,
      now,
    );
    return false;
  }
  db.query("UPDATE worker_identity SET active=0 WHERE session_id=?").run(
    sessionId,
  );
  return true;
};

/** Synchronous transactions make allocation atomic across gateway connections. */
export const makeWorkerIdentityRepo = (db: Database) => {
  const acquire = (sessionId: string, workspaceRoot?: string) =>
    tryDb(
      () =>
        db
          .transaction(() => {
            const range = workerUidRange(process.env);
            if (range === undefined) return undefined;
            if (workspaceRoot !== undefined) {
              const retired = db
                .query<{ session_id: string; uid: number }, [string, number]>(
                  `SELECT i.session_id, i.uid FROM worker_identity i JOIN agent_run r ON r.session_id=i.session_id
                 WHERE i.active=1 AND i.session_id<>? AND r.state IN ('succeeded','failed','canceled')
                 AND (r.lease_expires_at IS NULL OR r.lease_expires_at<=?)`,
                )
                .all(sessionId, Date.now());
              for (const row of retired)
                retireWorkerIdentitySync(
                  db,
                  row.session_id,
                  row.uid,
                  workspaceRoot,
                );
            }
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
            retireWorkerIdentitySync(db, sessionId, row.uid, workspaceRoot);
          })
          .immediate(),
      "WorkerIdentityRepo.release",
    );
  return { acquire, release };
};

export class WorkerIdentityRepo extends Context.Service<WorkerIdentityRepo>()(
  "WorkerIdentityRepo",
  {
    make: Effect.gen(function* () {
      const { db } = yield* SqliteClient;
      return makeWorkerIdentityRepo(db);
    }),
  },
) {
  static readonly layerWithoutDependencies = Layer.effect(this, this.make);
  static readonly layer = this.layerWithoutDependencies;
}
