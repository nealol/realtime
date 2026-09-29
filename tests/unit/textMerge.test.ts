import { describe, expect, it } from "vitest";
import { containsLines, mergeText, mergeWithoutBaseline } from "../../src/textMerge";

describe("mergeText", () => {
  it("merges simultaneous offline edits from two stale devices when they are disjoint", () => {
    expect(
      mergeText(
        "title\nfirst\nsecond\n",
        "local title\nfirst\nsecond\n",
        "title\nfirst\nremote second\n",
      ),
    ).toEqual({ kind: "merged", content: "local title\nfirst\nremote second\n" });
  });

  it("deduplicates the same edit made independently", () => {
    expect(mergeText("before\n", "after\n", "after\n")).toEqual({
      kind: "merged",
      content: "after\n",
    });
  });

  it("refuses to guess when edits overlap", () => {
    expect(mergeText("base\n", "local\n", "remote\n")).toEqual({ kind: "conflict" });
  });

  it("preserves exact trailing-newline state", () => {
    expect(mergeText("a\nb", "A\nb", "a\nB")).toEqual({
      kind: "merged",
      content: "A\nB",
    });
  });
});

describe("mergeWithoutBaseline", () => {
  const template = "# 2026-09-24\n\n## Tasks\n";
  const filled = "# 2026-09-24\n\n## Tasks\n- ship it\n";

  it("takes the remote when it already contains the local template", () => {
    expect(mergeWithoutBaseline(template, filled, true)).toEqual({
      kind: "merged",
      content: filled,
    });
  });

  it("takes the local when it contains the remote, unless remote deletions must win", () => {
    expect(mergeWithoutBaseline(filled, template, true)).toEqual({
      kind: "merged",
      content: filled,
    });
    expect(mergeWithoutBaseline(filled, template, false)).toEqual({ kind: "conflict" });
  });

  it("treats an empty local file as contained by any remote", () => {
    expect(mergeWithoutBaseline("", filled, false)).toEqual({ kind: "merged", content: filled });
  });

  it("conflicts when each side has lines the other lacks", () => {
    expect(mergeWithoutBaseline(`${template}- local\n`, filled, true)).toEqual({
      kind: "conflict",
    });
  });
});

describe("containsLines", () => {
  it("ignores blank lines and line terminators but keeps order", () => {
    expect(containsLines("a\r\nb\nc", "a\n\n\nc\n")).toBe(true);
    expect(containsLines("a\nb\nc", "c\na")).toBe(false);
    expect(containsLines("a\nb", "a\nb c")).toBe(false);
  });
});
