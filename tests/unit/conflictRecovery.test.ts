import { describe, expect, it } from "vitest";
import {
  preserveAdapterConflict,
  preserveBinaryConflict,
  preserveTextConflict,
  updateTextConflict,
} from "../../src/conflictRecovery";
import type RealtimePlugin from "../../src/main";
import { makeFakePlugin } from "../support/fakePlugin";

describe("conflict recovery copies", () => {
  it("writes text copies beside the source without overwriting an existing recovery", async () => {
    const { plugin, vault } = makeFakePlugin("https://sync.example.com", {});

    const first = await preserveTextConflict(plugin as any, "folder/note.md", "first", "remote");
    const second = await preserveTextConflict(plugin as any, "folder/note.md", "second", "remote");

    expect(first).toMatch(/^folder\/note \(conflicted copy Remote \d{8} \d{6}\)\.md$/);
    expect(second).toMatch(/^folder\/note \(conflicted copy Remote \d{8} \d{6}(?: 2)?\)\.md$/);
    expect(second).not.toBe(first);
    expect(vault.files.get(first)).toBe("first");
    expect(vault.files.get(second)).toBe("second");
  });

  it("keeps successive versions in one recovery copy", async () => {
    const { plugin, vault } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    // The fake implements the slice of the plugin recovery copies use.
    const realtime = plugin as unknown as RealtimePlugin;
    const copies = () => [...vault.files.keys()].filter((path) => path.includes("conflicted copy"));

    const first = await updateTextConflict(realtime, "note.md", "one", "local", null);
    const second = await updateTextConflict(realtime, "note.md", "one two", "local", first);
    expect(second).toBe(first);
    expect(copies()).toEqual([first]);
    expect(vault.files.get(first!)).toBe("one two");

    // A later session finds a copy that already holds its content.
    expect(await updateTextConflict(realtime, "note.md", "one two", "local", null)).toBeNull();
    expect(copies()).toEqual([first]);

    // A copy deleted meanwhile is recreated rather than silently lost.
    vault.files.delete(first!);
    const third = await updateTextConflict(realtime, "note.md", "one two three", "local", first);
    expect(copies()).toEqual([third]);
    expect(vault.files.get(third!)).toBe("one two three");
  });

  it("preserves binary bytes exactly", async () => {
    const { plugin, vault } = makeFakePlugin("https://sync.example.com", {});
    const data = new Uint8Array([0, 255, 4, 9]).buffer;

    const path = await preserveBinaryConflict(plugin as any, "image.png", data, "local");

    expect(path).toMatch(/^image \(conflicted copy Test Client \d{8} \d{6}\)\.png$/);
    expect([...new Uint8Array(vault.binaries.get(path)!)]).toEqual([0, 255, 4, 9]);
  });

  it("writes hidden config recovery copies through the vault adapter", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {});
    const stored = new Map<string, ArrayBuffer>();
    (plugin.app.vault as any).adapter = {
      exists: async (path: string) => stored.has(path),
      writeBinary: async (path: string, content: ArrayBuffer) => {
        stored.set(path, content);
      },
    };
    const data = new Uint8Array([7, 8, 9]).buffer;

    const path = await preserveAdapterConflict(
      plugin as any,
      ".obsidian/appearance.json",
      data,
      "local",
    );

    expect(path).toMatch(
      /^\.obsidian\/appearance \(conflicted copy Test Client \d{8} \d{6}\)\.json$/,
    );
    expect([...new Uint8Array(stored.get(path)!)]).toEqual([7, 8, 9]);
  });
});
