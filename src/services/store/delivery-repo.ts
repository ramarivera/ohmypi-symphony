import { createHash, randomUUID } from "node:crypto";
import { Clock, Context, Effect, Layer, Option, Schema } from "effect";
import { DatabaseError, type RowDecodeError } from "../../domain/errors.js";
import { DeliveryId } from "../../domain/ids.js";
import {
  decodeRow,
  decodeRows,
  runChanges,
  SqliteClient,
  transact,
  tryDb,
} from "./sqlite-client.js";

const DeliveryIdentityRow = Schema.Struct({
  organization_id: Schema.String,
  payload_hash: Schema.String,
  status: Schema.Literals(["pending", "processed", "failed"]),
});

type DeliveryIdentityRow = Schema.Schema.Type<typeof DeliveryIdentityRow>;

export type DeliveryClaimResult = "claimed" | "duplicate" | "conflict";

// Bind the normalized stored JSON to its ingress hash, tenant and format.
// payload_hash remains the hash of the original HTTP bytes for sender dedupe.
export const deliveryReplayHash = (
  organizationId: string,
  ingressHash: string,
  payloadJson: string,
  version: number,
): string =>
  createHash("sha256")
    .update(JSON.stringify([version, organizationId, ingressHash, payloadJson]))
    .digest("hex");

const PendingDelivery = Schema.Struct({
  delivery_id: DeliveryId,
  organization_id: Schema.String,
  payload_hash: Schema.String,
  payload_json: Schema.String,
  payload_version: Schema.Number,
  replay_hash: Schema.NullOr(Schema.String),
});
export type PendingDelivery = Schema.Schema.Type<typeof PendingDelivery>;

export class DeliveryRepo extends Context.Service<DeliveryRepo>()(
  "DeliveryRepo",
  {
    make: Effect.gen(function* () {
      const { db } = yield* SqliteClient;

      const accept = Effect.fn("DeliveryRepo.accept")(function* (input: {
        readonly id: DeliveryId;
        readonly organizationId: string;
        readonly payloadHash: string;
        readonly payload: unknown;
        readonly receivedAt?: number;
        readonly claimToken?: string;
        readonly payloadVersion?: 1;
      }): Effect.fn.Return<boolean, DatabaseError> {
        yield* Effect.annotateCurrentSpan("deliveryId", input.id);
        const now = input.receivedAt ?? (yield* Clock.currentTimeMillis);
        const json = JSON.stringify(input.payload);
        const version = input.payloadVersion ?? 0;
        const result = yield* tryDb(
          () =>
            db
              .query(`
                INSERT INTO webhook_delivery (delivery_id, organization_id, received_at, payload_hash, payload_json, payload_version, replay_hash, claim_token)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(delivery_id) DO NOTHING
              `)
              .run(
                input.id,
                input.organizationId,
                now,
                input.payloadHash,
                json,
                version,
                deliveryReplayHash(
                  input.organizationId,
                  input.payloadHash,
                  json,
                  version,
                ),
                input.claimToken ?? randomUUID(),
              ),
          "DeliveryRepo.accept",
        );
        return (yield* runChanges(result, "DeliveryRepo.accept")) === 1;
      });

      const claim = Effect.fn("DeliveryRepo.claim")(function* (input: {
        readonly id: DeliveryId;
        readonly organizationId: string;
        readonly payloadHash: string;
        readonly payload: unknown;
        readonly receivedAt?: number;
        readonly claimToken?: string;
        readonly payloadVersion?: 1;
      }): Effect.fn.Return<
        DeliveryClaimResult,
        DatabaseError | RowDecodeError
      > {
        yield* Effect.annotateCurrentSpan("deliveryId", input.id);

        const tx = Effect.gen(function* () {
          const accepted = yield* accept(input);
          if (accepted) return "claimed" as const;

          const existing = yield* tryDb(
            () =>
              db
                .query<DeliveryIdentityRow, [string]>(
                  "SELECT organization_id, payload_hash, status FROM webhook_delivery WHERE delivery_id=?",
                )
                .get(input.id),
            "DeliveryRepo.claim.select",
          );
          if (existing === null) {
            return yield* Effect.fail(
              new DatabaseError({
                message: `Delivery ${input.id} disappeared`,
              }),
            );
          }
          const decoded = yield* decodeRow(
            DeliveryIdentityRow,
            existing,
            "Delivery",
          );

          if (
            decoded.payload_hash !== input.payloadHash ||
            decoded.organization_id !== input.organizationId
          ) {
            return "conflict" as const;
          }
          if (decoded.status !== "failed") {
            return "duplicate" as const;
          }

          const result = yield* tryDb(
            () =>
              db
                .query(
                  "UPDATE webhook_delivery SET status='pending', error=NULL, payload_json=?, payload_version=?, replay_hash=?, claim_token=? WHERE delivery_id=? AND status='failed'",
                )
                .run(
                  JSON.stringify(input.payload),
                  input.payloadVersion ?? 0,
                  deliveryReplayHash(
                    input.organizationId,
                    input.payloadHash,
                    JSON.stringify(input.payload),
                    input.payloadVersion ?? 0,
                  ),
                  input.claimToken ?? randomUUID(),
                  input.id,
                ),
            "DeliveryRepo.claim.reset",
          );
          return (result as { changes: number }).changes === 1
            ? "claimed"
            : "duplicate";
        });

        return yield* transact(db, tx);
      });

      const mark = Effect.fn("DeliveryRepo.mark")(function* (
        id: DeliveryId,
        status: "processed" | "failed",
        error?: string,
        claimToken?: string,
      ): Effect.fn.Return<void, DatabaseError> {
        yield* Effect.annotateCurrentSpan("deliveryId", id);
        yield* tryDb(
          () =>
            db
              .query(
                "UPDATE webhook_delivery SET status=?, error=? WHERE delivery_id=? AND status='pending' AND (? IS NULL OR claim_token=?)",
              )
              .run(
                status,
                error ?? null,
                id,
                claimToken ?? null,
                claimToken ?? null,
              ),
          "DeliveryRepo.mark",
        );
      });

      const pendingIds = Effect.fn("DeliveryRepo.pendingIds")(function* () {
        const rows = yield* tryDb(
          () =>
            db
              .query<{ delivery_id: string }, []>(
                "SELECT delivery_id FROM webhook_delivery WHERE status='pending' ORDER BY received_at, delivery_id",
              )
              .all(),
          "DeliveryRepo.pendingIds",
        );
        return yield* decodeRows(
          Schema.Struct({ delivery_id: Schema.String }),
          rows,
          "Delivery",
        );
      });

      // Rotate ownership before replay. A stale ingress/recovery fiber can no
      // longer mutate effects or mark completion after this transaction commits.
      const claimReplay = Effect.fn("DeliveryRepo.claimReplay")(function* (
        id: DeliveryId,
        token: string,
      ) {
        const claimed = yield* transact(
          db,
          Effect.gen(function* () {
            const result = yield* tryDb(
              () =>
                db
                  .query(
                    "UPDATE webhook_delivery SET claim_token=? WHERE delivery_id=? AND status='pending'",
                  )
                  .run(token, id),
              "DeliveryRepo.claimReplay",
            );
            if ((yield* runChanges(result, "DeliveryRepo.claimReplay")) === 0)
              return Option.none<unknown>();
            const row = yield* tryDb(
              () =>
                db
                  .query<PendingDelivery, [string]>(
                    "SELECT delivery_id, organization_id, payload_hash, payload_json, payload_version, replay_hash FROM webhook_delivery WHERE delivery_id=?",
                  )
                  .get(id),
              "DeliveryRepo.claimReplay.select",
            );
            return Option.some<unknown>(row);
          }),
        );
        if (Option.isNone(claimed)) return Option.none<PendingDelivery>();
        return Option.some(
          yield* decodeRow(PendingDelivery, claimed.value, "Delivery"),
        );
      });

      const processClaim = <A, E, R>(
        id: DeliveryId,
        token: string,
        effect: Effect.Effect<A, E, R>,
      ) =>
        transact(
          db,
          Effect.gen(function* () {
            const owned = yield* tryDb(
              () =>
                db
                  .query<unknown, [string, string]>(
                    "SELECT 1 FROM webhook_delivery WHERE delivery_id=? AND status='pending' AND claim_token=?",
                  )
                  .get(id, token),
              "DeliveryRepo.processClaim.fence",
            );
            if (owned === null) return false;
            yield* effect;
            yield* mark(id, "processed", undefined, token);
            return true;
          }),
        );

      const recoverPendingDeliveries = Effect.fn(
        "DeliveryRepo.recoverPendingDeliveries",
      )(function* (
        reason = "Gateway restarted before webhook processing completed",
      ): Effect.fn.Return<number, DatabaseError> {
        const result = yield* tryDb(
          () =>
            db
              .query(
                "UPDATE webhook_delivery SET status='failed', error=?, claim_token=NULL WHERE status='pending'",
              )
              .run(reason),
          "DeliveryRepo.recoverPendingDeliveries",
        );
        return yield* runChanges(
          result,
          "DeliveryRepo.recoverPendingDeliveries",
        );
      });

      return {
        accept,
        claim,
        mark,
        pendingIds,
        claimReplay,
        processClaim,
        recoverPendingDeliveries,
      };
    }),
  },
) {
  static readonly layerWithoutDependencies = Layer.effect(this, this.make);
  static readonly layer = this.layerWithoutDependencies;
}
