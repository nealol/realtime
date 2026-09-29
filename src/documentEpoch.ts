import type RealtimePlugin from "./main";

const STORAGE_PREFIX = "realtime:document-epoch";
const fallbackEpochs = new Map<string, number>();

type EpochProposalHandler = (documentId: string, epoch: number) => void;
let proposalHandler: EpochProposalHandler | null = null;

function storageKey(plugin: RealtimePlugin, documentId: string): string {
  const server = plugin.settings.authServerId || plugin.settings.authServerUrl;
  return `${STORAGE_PREFIX}:${server}:${documentId}`;
}

export function getDocumentEpoch(plugin: RealtimePlugin, documentId: string): number {
  const key = storageKey(plugin, documentId);
  const raw =
    typeof localStorage === "undefined" ? fallbackEpochs.get(key) : localStorage.getItem(key);
  const epoch = typeof raw === "number" ? raw : raw === null ? 0 : Number(raw);
  return Number.isSafeInteger(epoch) && epoch >= 0 ? epoch : 0;
}

export function setDocumentEpoch(
  plugin: RealtimePlugin,
  documentId: string,
  epoch: number,
): boolean {
  if (!Number.isSafeInteger(epoch) || epoch < 0) {
    throw new Error(`Realtime: invalid document epoch ${String(epoch)} for "${documentId}".`);
  }
  const key = storageKey(plugin, documentId);
  const previous = getDocumentEpoch(plugin, documentId);
  if (epoch < previous) {
    throw new Error(
      `Realtime: document epoch cannot move backward from ${previous} to ${epoch} for "${documentId}".`,
    );
  }
  if (typeof localStorage === "undefined") fallbackEpochs.set(key, epoch);
  else localStorage.setItem(key, String(epoch));
  return previous !== epoch;
}

export function epochPersistenceName(
  plugin: RealtimePlugin,
  documentId: string,
  baseName: string,
  epoch = getDocumentEpoch(plugin, documentId),
): string {
  return epoch === 0 ? baseName : `${baseName}:epoch:${epoch}`;
}

export function setEpochProposalHandler(handler: EpochProposalHandler | null): void {
  proposalHandler = handler;
}

export function handleEpochProposal(documentId: string, epoch: number): void {
  if (!proposalHandler) {
    throw new Error(
      `Realtime: received epoch ${epoch} for "${documentId}" before sync initialized.`,
    );
  }
  proposalHandler(documentId, epoch);
}

export function resetDocumentEpochStateForTests(): void {
  fallbackEpochs.clear();
  proposalHandler = null;
}
