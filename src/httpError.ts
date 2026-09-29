// Kept free of `obsidian` imports so low-level sync modules (and the test
// setup that loads them) can classify failures without pulling in auth.ts.

/** A non-2xx response (other than a rejected session) from the server. */
export class HttpError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

/**
 * Statuses that say the server (or something in front of it) is unavailable
 * right now, as opposed to rejecting this particular request.
 */
export function isServerUnavailableStatus(status: number): boolean {
  return status === 408 || status === 429 || status === 502 || status === 503 || status === 504;
}
