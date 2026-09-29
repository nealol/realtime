import { describe, it, expect } from "vitest";
import * as Y from "yjs";
import { applyTextToYText } from "../../src/diff";

function apply(initial: string, target: string): string {
  const doc = new Y.Doc();
  const text = doc.getText("contents");
  if (initial) text.insert(0, initial);
  doc.transact(() => applyTextToYText(text, target, "test"));
  return text.toString();
}

describe("applyTextToYText", () => {
  it("inserts into empty text", () => {
    expect(apply("", "hello world")).toBe("hello world");
  });

  it("is a no-op when unchanged", () => {
    const doc = new Y.Doc();
    const text = doc.getText("contents");
    text.insert(0, "same");
    let changed = false;
    text.observe(() => (changed = true));
    doc.transact(() => applyTextToYText(text, "same", "test"));
    expect(text.toString()).toBe("same");
    expect(changed).toBe(false);
  });

  it("appends at the end (common prefix)", () => {
    expect(apply("hello", "hello world")).toBe("hello world");
  });

  it("prepends at the start (common suffix)", () => {
    expect(apply("world", "hello world")).toBe("hello world");
  });

  it("replaces only the differing middle", () => {
    expect(apply("the quick brown fox", "the slow brown fox")).toBe("the slow brown fox");
  });

  it("handles a pure deletion", () => {
    expect(apply("hello world", "hello")).toBe("hello");
  });

  it("preserves a concurrent edit outside the changed region", () => {
    // Two docs sharing a baseline; one rewrites the middle via the diff, the
    // other appends — the CRDT keeps both.
    const a = new Y.Doc();
    const ta = a.getText("contents");
    ta.insert(0, "the quick brown fox");
    const b = new Y.Doc();
    Y.applyUpdate(b, Y.encodeStateAsUpdate(a));
    const tb = b.getText("contents");

    a.transact(() => applyTextToYText(ta, "the slow brown fox", "test"));
    tb.insert(tb.length, "!"); // concurrent append on b

    Y.applyUpdate(a, Y.encodeStateAsUpdate(b));
    Y.applyUpdate(b, Y.encodeStateAsUpdate(a));
    expect(ta.toString()).toBe(tb.toString());
    expect(ta.toString()).toBe("the slow brown fox!");
  });
});

/** Apply the diff on one peer, round-trip the update, and return both texts. */
function applyAndSync(initial: string, target: string, separateItems = false) {
  const local = new Y.Doc();
  const text = local.getText("contents");
  if (separateItems) {
    for (const char of Array.from(initial)) text.insert(text.length, char);
  } else if (initial) {
    text.insert(0, initial);
  }
  local.transact(() => applyTextToYText(text, target));
  const remote = new Y.Doc();
  Y.applyUpdate(remote, Y.encodeStateAsUpdate(local));
  return { local: text.toString(), remote: remote.getText("contents").toString() };
}

describe("applyTextToYText with astral characters", () => {
  it.each([
    ["replaces one emoji with another from the same block", "Status: 🟢 done", "Status: 🔴 done"],
    ["inserts an emoji before another from the same block", "x😀y", "x😃😀y"],
    ["deletes an emoji followed by another from the same block", "😃😀", "😀"],
    ["deletes an emoji preceded by another from the same block", "😀😃", "😀"],
    ["replaces a character whose low surrogate is unchanged", "\u{1F389}", "\u{1F789}"],
    ["appends after an emoji", "😀", "😀😃"],
  ])("%s", (_label, initial, target) => {
    for (const separateItems of [false, true]) {
      const { local, remote } = applyAndSync(initial, target, separateItems);
      expect(local).toBe(target);
      expect(remote).toBe(target);
    }
  });

  it("never corrupts random edits of emoji-heavy text", () => {
    const alphabet = ["a", "b", " ", "\n", "😀", "😃", "🟢", "🔴", "🎉", "é", "中"];
    let seed = 42;
    const random = () => {
      seed = (seed * 1103515245 + 12345) % 2 ** 31;
      return seed / 2 ** 31;
    };
    const word = (length: number) =>
      Array.from({ length }, () => alphabet[Math.floor(random() * alphabet.length)]).join("");
    for (let round = 0; round < 500; round++) {
      const initial = Array.from(word(1 + Math.floor(random() * 12)));
      const edited = [...initial];
      const at = Math.floor(random() * (edited.length + 1));
      const remove = Math.floor(random() * Math.min(3, edited.length - at + 1));
      edited.splice(at, remove, ...Array.from(word(Math.floor(random() * 3))));
      const target = edited.join("");
      const { local, remote } = applyAndSync(initial.join(""), target, random() < 0.5);
      expect(local).toBe(target);
      expect(remote).toBe(target);
    }
  });
});
