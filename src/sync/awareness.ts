import type * as Y from "yjs";
import { Awareness, outdatedTimeout, removeAwarenessStates } from "y-protocols/awareness";

/**
 * y-protocols gives every Awareness its own 3s interval that renews the local
 * state and expires silent peers. Every synced document owns an Awareness, so
 * a large vault woke the app hundreds of times a second while idle. One
 * shared interval runs the same check for every instance instead, skipping
 * the (usual) ones that hold no state at all.
 */
const CHECK_INTERVAL_MS = outdatedTimeout / 10;

const tracked = new Set<Awareness>();
let checkTimer: ReturnType<typeof setInterval> | null = null;

/** An Awareness for `doc` whose expiry check runs on the shared interval. */
export function createAwareness(doc: Y.Doc): Awareness {
  const awareness = new Awareness(doc);
  clearInterval(awareness._checkInterval);
  tracked.add(awareness);
  awareness.on("destroy", () => {
    tracked.delete(awareness);
    if (tracked.size === 0 && checkTimer !== null) {
      clearInterval(checkTimer);
      checkTimer = null;
    }
  });
  checkTimer ??= setInterval(checkAll, CHECK_INTERVAL_MS);
  return awareness;
}

/** y-protocols' per-instance check, applied to every tracked Awareness. */
function checkAll(): void {
  const now = Date.now();
  for (const awareness of tracked) {
    if (awareness.states.size === 0) continue;
    const local = awareness.getLocalState();
    const localMeta = awareness.meta.get(awareness.clientID);
    if (local !== null && localMeta && outdatedTimeout / 2 <= now - localMeta.lastUpdated) {
      awareness.setLocalState(local);
    }
    const expired: number[] = [];
    awareness.meta.forEach((meta, clientId) => {
      if (
        clientId !== awareness.clientID &&
        outdatedTimeout <= now - meta.lastUpdated &&
        awareness.states.has(clientId)
      ) {
        expired.push(clientId);
      }
    });
    if (expired.length > 0) removeAwarenessStates(awareness, expired, "timeout");
  }
}
