import { Clock, Effect, Schema } from "effect";
import type { DeliveryId } from "../../domain/ids.js";
import {
  DatabaseError,
  RowDecodeError,
} from "../../domain/errors.js";
import { SqliteClient, tryDb, runChanges, decodeRow, transact } from "./sqlite-client.js";

const DeliveryIdentityRow = Schema.Struct({
  payload_hash: Schema.String,
  status: Schema.Literal("pending", "processed", "failed"),
});

type DeliveryIdentityRow = Schema.Schema.Type<typeof DeliveryIdentityRow>;

export type DeliveryClaimResult = "claimed" | "duplicate" | "conflict";

export class DeliveryRepo extends Effect.Service<DeliveryRepo>()(
  "DeliveryRepo",
  {
    accessors: true,
    effect: Effect.gen(function* () {
      const { db } = yield* SqliteClient;

      const accept = Effect.fn("DeliveryRepo.accept")(
        function* (input: {
          readonly id: DeliveryId;
          readonly organizationId: string;
          readonly payloadHash: string;
          readonly payload: unknown;
          readonly receivedAt?: number;
        }) { yield* Effect.annotateCurrentSpan("deliveryId", input.id);
        const now = input.receivedAt ?? (yield* Clock.currentTimeMillis);
        const result = yield* tryDb(
          () =>
            db
              .query(`
                INSERT INTO webhook_delivery (delivery_id, organization_id, received_at, payload_hash, payload_json)
                VALUES (?, ?, ?, ?, ?) ON CONFLICT(delivery_id) DO NOTHING
              `)
              .run(
                input.id,
                input.organizationId,
                now,
                input.payloadHash,
                JSON.stringify(input.payload),
              ),
          "DeliveryRepo.accept",
        );
        return (yield* runChanges(result, "DeliveryRepo.accept")) === 1; },
      );

      const claim = Effect.fn("DeliveryRepo.claim")(
        function* (input: {
          readonly id: DeliveryId;
          readonly organizationId: string;
          readonly payloadHash: string;
          readonly payload: unknown;
          readonly receivedAt?: number;
        }) { yield* Effect.annotateCurrentSpan("deliveryId", input.id);

        const tx = Effect.gen(function* () {
          const accepted = yield* accept(input);
          if (accepted) return "claimed" as const;

          const existing = yield* tryDb(
            () =>
              db
                .query<DeliveryIdentityRow, [string]>(
                  "SELECT payload_hash, status FROM webhook_delivery WHERE delivery_id=?",
                )
                .get(input.id),
            "DeliveryRepo.claim.select",
          );
          if (existing === null) {
            return yield* Effect.fail(new DatabaseError({ message: `Delivery ${input.id} disappeared` }))
          }
          const decoded = yield* decodeRow(
            DeliveryIdentityRow,
            existing,
            "Delivery",
          );

          if (decoded.payload_hash !== input.payloadHash) {
            return "conflict" as const;
          }
          if (decoded.status !== "failed") {
            return "duplicate" as const;
          }

          const result = yield* tryDb(
            () =>
              db
                .query(
                  "UPDATE webhook_delivery SET status='pending', error=NULL WHERE delivery_id=? AND status='failed'",
                )
                .run(input.id),
            "DeliveryRepo.claim.reset",
          );
          return (result as { changes: number }).changes === 1
            ? "claimed"
            : "duplicate";
        });

        return yield* transact(db, tx); },
      );

      const mark = Effect.fn("DeliveryRepo.mark")(
        function* (id: DeliveryId,
        status: "processed" | "failed",
        error?: string,) { yield* Effect.annotateCurrentSpan("deliveryId", id);
        yield* tryDb(() =>
          db
            .query(
              "UPDATE webhook_delivery SET status=?, error=? WHERE delivery_id=?",
            )
            .run(status, error ?? null, id), "DeliveryRepo.mark"); },
      );

      const recoverPendingDeliveries = Effect.fn(
        "DeliveryRepo.recoverPendingDeliveries",
      )(
        function* (reason = "Gateway restarted before webhook processing completed",) { const result = yield* tryDb(
          () =>
            db
              .query(
                "UPDATE webhook_delivery SET status='failed', error=? WHERE status='pending'",
              )
              .run(reason),
          "DeliveryRepo.recoverPendingDeliveries",
        );
        return yield* runChanges(result, "DeliveryRepo.recoverPendingDeliveries"); },
      );

      return { accept, claim, mark, recoverPendingDeliveries };
    }),
  }
) {}
