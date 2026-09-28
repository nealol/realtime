/**
 * Attachment embeds in Markdown notes, resolved against the synced binary
 * index rather than the local vault: on a fresh device the images an open
 * note shows are exactly the ones not on disk yet, so Obsidian's own link
 * resolution cannot find them.
 */

const WIKI_EMBED = /!\[\[([^\]\n]+?)\]\]/g;
const MARKDOWN_EMBED = /!\[[^\]\n]*\]\(\s*(<[^>\n]+>|[^)\s]+)(?:\s+(?:"[^"\n]*"|'[^'\n]*'))?\s*\)/g;
const FENCED_CODE = /^(```|~~~)[^\n]*\n[\s\S]*?^\1[^\n]*$/gm;
const INLINE_CODE = /`[^`\n]*`/g;
const URL_SCHEME = /^[a-z][a-z0-9+.-]*:/i;

/** Link targets of `![[…]]` and `![…](…)` embeds, without aliases or subpaths. */
export function embeddedLinkpaths(markdown: string): string[] {
  const text = markdown.replace(FENCED_CODE, "").replace(INLINE_CODE, "");
  const out = new Set<string>();
  for (const match of text.matchAll(WIKI_EMBED)) {
    const target = match[1].split("|")[0].split("#")[0].trim();
    if (target) out.add(target);
  }
  for (const match of text.matchAll(MARKDOWN_EMBED)) {
    let target = match[1];
    if (target.startsWith("<")) target = target.slice(1, -1);
    if (URL_SCHEME.test(target)) continue;
    target = target.split("#")[0];
    try {
      target = decodeURI(target);
    } catch {
      // Keep the raw target; a stray "%" is a legal file-name character.
    }
    target = target.trim();
    if (target) out.add(target);
  }
  return [...out];
}

function folderOf(path: string): string {
  const slash = path.lastIndexOf("/");
  return slash < 0 ? "" : path.slice(0, slash);
}

function joinRelative(folder: string, relative: string): string | null {
  const parts = folder ? folder.split("/") : [];
  for (const part of relative.split("/")) {
    if (part === "" || part === ".") continue;
    if (part === "..") {
      if (!parts.length) return null;
      parts.pop();
    } else {
      parts.push(part);
    }
  }
  return parts.join("/");
}

/** Shortest path wins, then lexical order — Obsidian's tie-break for names. */
function pickBest(matches: string[], sourceFolder: string): string | null {
  if (!matches.length) return null;
  const sameFolder = matches.filter((path) => folderOf(path) === sourceFolder);
  const pool = sameFolder.length ? sameFolder : matches;
  return [...pool].sort((a, b) => a.length - b.length || (a < b ? -1 : a > b ? 1 : 0))[0];
}

/**
 * Resolve an embed target the way Obsidian does, against `candidates`:
 * an exact or note-relative path first, then a path suffix, then the file
 * name alone (preferring the note's own folder). Case-insensitive fallback.
 */
export function resolveAttachmentLink(
  linkpath: string,
  sourcePath: string,
  candidates: readonly string[],
): string | null {
  const sourceFolder = folderOf(sourcePath);
  const link = linkpath.replace(/^\/+/, "");
  if (!link) return null;
  for (const fold of [false, true]) {
    const key = (path: string) => (fold ? path.toLowerCase() : path);
    const byPath = new Map(candidates.map((path) => [key(path), path]));
    const relative = joinRelative(sourceFolder, link);
    const exact =
      (relative !== null ? byPath.get(key(relative)) : undefined) ?? byPath.get(key(link));
    if (exact) return exact;
    const wanted = key(link);
    const matches = link.includes("/")
      ? candidates.filter((path) => key(path).endsWith(`/${wanted}`))
      : candidates.filter((path) => key(path.slice(path.lastIndexOf("/") + 1)) === wanted);
    const best = pickBest(matches, sourceFolder);
    if (best) return best;
  }
  return null;
}
