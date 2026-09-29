import type * as Y from "yjs";

function isHighSurrogate(code: number): boolean {
  return code >= 0xd800 && code <= 0xdbff;
}

function isLowSurrogate(code: number): boolean {
  return code >= 0xdc00 && code <= 0xdfff;
}

/**
 * Applies `newText` to a Y.Text by replacing only the region that actually
 * changed. We trim the common prefix and suffix and rewrite the middle in a
 * single delete+insert. This keeps deltas small and avoids clobbering the whole
 * document (which would disrupt other editors' relative cursor positions).
 *
 * The changed region never starts or ends inside a UTF-16 surrogate pair: Yjs
 * replaces both halves of a split pair with U+FFFD, which would corrupt emoji
 * and other astral characters next to the edit (and diverge from peers).
 *
 * Must be called inside a `ydoc.transact(..., origin)` — the caller owns the
 * transaction so the origin is consistent and operation ordering is predictable.
 */
export function applyTextToYText(ytext: Y.Text, newText: string): void {
  const oldText = ytext.toString();
  if (oldText === newText) return;

  const oldLen = oldText.length;
  const newLen = newText.length;

  // Common prefix length.
  let start = 0;
  const maxStart = Math.min(oldLen, newLen);
  while (start < maxStart && oldText[start] === newText[start]) {
    start++;
  }
  // Do not split a surrogate pair at the start of the changed region.
  if (start > 0 && isHighSurrogate(oldText.charCodeAt(start - 1))) start--;

  // Common suffix length (not overlapping the prefix).
  let endOld = oldLen;
  let endNew = newLen;
  while (endOld > start && endNew > start && oldText[endOld - 1] === newText[endNew - 1]) {
    endOld--;
    endNew--;
  }
  // Nor at its end: a suffix that begins with a low surrogate belongs to a
  // pair whose high half is being replaced.
  if (endOld < oldLen && isLowSurrogate(oldText.charCodeAt(endOld))) {
    endOld++;
    endNew++;
  }

  const deleteCount = endOld - start;
  const insert = newText.slice(start, endNew);

  if (deleteCount > 0) ytext.delete(start, deleteCount);
  if (insert.length > 0) ytext.insert(start, insert);
}
