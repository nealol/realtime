import type { Awareness } from "y-protocols/awareness";

/** Something that decides whether an awareness instance publishes this client's presence. */
export interface PresenceOwner {
  /** Publish presence until the returned function is called. */
  holdPresence(): () => void;
}

const owners = new WeakMap<Awareness, PresenceOwner>();

/** Declare `owner` responsible for `awareness`'s local presence. */
export function registerPresenceOwner(awareness: Awareness, owner: PresenceOwner): void {
  owners.set(awareness, owner);
}

/**
 * Keep presence published on `awareness` while a view of its document is
 * open. Awareness with no registered owner always publishes; the returned
 * function releases the hold.
 */
export function holdPresence(awareness: Awareness): () => void {
  return owners.get(awareness)?.holdPresence() ?? (() => {});
}
