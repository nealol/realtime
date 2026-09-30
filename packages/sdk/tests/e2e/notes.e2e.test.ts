import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  ConflictError,
  NotFoundError,
  RealtimeClient,
  noteContentHash,
  type VaultHandle,
} from "../../src/index";
import { startAuthHarness, type AuthHarness } from "../support/harness";

let harness: AuthHarness;
let token: string;
let client: RealtimeClient;
let vault: VaultHandle;

beforeAll(async () => {
  harness = await startAuthHarness();
  token = await harness.loginUser("alice");
  client = new RealtimeClient({ baseUrl: harness.authUrl, token });
  vault = client.vault((await client.vaults.create("Notes Vault")).id);
});

afterAll(async () => {
  await harness?.stop();
});

describe("notes CRUD", () => {
  it("creates, reads, replaces, patches, appends, moves, and deletes", async () => {
    const created = await vault.notes.create("Folder/Hello World.md", "# Hello\n\nfirst");
    expect(created.guid).toBeTruthy();
    expect(created.content).toContain("# Hello");

    const read = await vault.notes.read("Folder/Hello World.md");
    expect(read.content).toBe("# Hello\n\nfirst");

    const replaced = await vault.notes.replace("Folder/Hello World.md", "# Hello\n\nsecond");
    expect(replaced.content).toBe("# Hello\n\nsecond");

    const patched = await vault.notes.patch("Folder/Hello World.md", {
      old: "second",
      new: "third",
    });
    expect(patched.content).toBe("# Hello\n\nthird");

    const appended = await vault.notes.append("Folder/Hello World.md", "- tail");
    expect(appended.content).toBe("# Hello\n\nthird\n- tail");

    const moved = await vault.notes.move("Folder/Hello World.md", "Archive/Hello.md");
    expect(moved.path).toBe("Archive/Hello.md");

    const listed = await vault.notes.list();
    expect(listed.map((n) => n.path)).toContain("Archive/Hello.md");

    await vault.notes.delete("Archive/Hello.md");
    await expect(vault.notes.read("Archive/Hello.md")).rejects.toThrow(NotFoundError);
  });

  it("mints a permalink", async () => {
    await vault.notes.create("Perma.md", "x");
    const link = await vault.notes.permalink("Perma.md");
    expect(link.url).toContain("/n/");
  });

  it("refuses a replace built on stale content", async () => {
    await vault.notes.create("Stale.md", "# Log\n");
    const basis = await noteContentHash("# Log\n");
    await vault.notes.patch("Stale.md", { old: "# Log", new: "# Team log" });

    await expect(
      vault.notes.replace("Stale.md", "# Log\nclobbered\n", { expectedContentHash: basis }),
    ).rejects.toThrow(ConflictError);
    expect((await vault.notes.read("Stale.md")).content).toBe("# Team log\n");

    const current = await noteContentHash("# Team log\n");
    const replaced = await vault.notes.replace("Stale.md", "# Team log\nkept\n", {
      expectedContentHash: current,
    });
    expect(replaced.content).toBe("# Team log\nkept\n");
  });

  it("appends after an edit that lands between its read and its write", async () => {
    await vault.notes.create("Race.md", "# Log\n");
    let raced = false;
    const racing = new RealtimeClient({
      baseUrl: harness.authUrl,
      token,
      fetch: async (input, init) => {
        if (!raced && init?.method === "PUT") {
          raced = true;
          await vault.notes.patch("Race.md", { old: "# Log", new: "# Team log" });
        }
        return fetch(input, init);
      },
    });

    const appended = await racing.vault(vault.vaultId).notes.append("Race.md", "- entry");
    expect(raced).toBe(true);
    expect(appended.content).toBe("# Team log\n- entry");
  });
});

describe("frontmatter", () => {
  it("parses and patches frontmatter", async () => {
    await vault.notes.create("Meta.md", "---\nstatus: draft\n---\nbody");
    const parsed = await vault.frontmatter.parse("Meta.md");
    expect(parsed.frontmatter).toMatchObject({ status: "draft" });

    const updated = await vault.frontmatter.patch("Meta.md", {
      set: { status: "done", tags: ["a", "b"] },
      unset: [],
    });
    expect(updated.content).toContain("status: done");

    const reparsed = await vault.frontmatter.parse("Meta.md");
    expect(reparsed.frontmatter).toMatchObject({ status: "done", tags: ["a", "b"] });
  });
});

describe("periodic notes", () => {
  it("gets-or-creates and appends to the daily note", async () => {
    const daily = await vault.periodic.getOrCreate("daily", { content: "## Log\n" });
    expect(daily.path).toMatch(/\d{4}-\d{2}-\d{2}\.md$/);
    const appended = await vault.periodic.append("daily", "- entry");
    expect(appended.content).toContain("- entry");
  });
});

describe("search", () => {
  it("indexes notes for search, tags, and backlinks", async () => {
    await vault.notes.create("Search/Target.md", "# Target\n\n#findme searchable-haystack");
    await vault.notes.create("Search/Source.md", "links to [[Target]]");
    const { count } = await vault.search.reindex();
    expect(count).toBeGreaterThan(0);

    const hits = await vault.search.search("searchable-haystack");
    expect(hits.map((h) => h.path)).toContain("Search/Target.md");

    const tags = await vault.search.tags();
    expect(tags.find((t) => t.tag === "findme" || t.tag === "#findme")?.count).toBeGreaterThan(0);

    const backlinks = await vault.search.backlinks("Search/Target.md");
    expect(backlinks.map((b) => b.path)).toContain("Search/Source.md");
  });
});

describe("canvases and bases", () => {
  it("creates a canvas and edits nodes/edges", async () => {
    await vault.canvases.create("Board.canvas");
    const withNode = await vault.canvases.addNode("Board.canvas", {
      id: "n1",
      type: "text",
      text: "hello",
      x: 0,
      y: 0,
      width: 100,
      height: 50,
    });
    expect(withNode.kind).toBe("canvas");
    await vault.canvases.addNode("Board.canvas", {
      id: "n2",
      type: "text",
      text: "world",
      x: 200,
      y: 0,
      width: 100,
      height: 50,
    });
    const withEdge = await vault.canvases.addEdge("Board.canvas", {
      id: "e1",
      fromNode: "n1",
      toNode: "n2",
    });
    const value = withEdge.value as { nodes: { id: string }[]; edges: { id: string }[] };
    expect(value.nodes.map((n) => n.id).sort()).toEqual(["n1", "n2"]);
    expect(value.edges[0].id).toBe("e1");

    const patched = await vault.canvases.updateNode("Board.canvas", "n1", { text: "hi" });
    const node = (patched.value as { nodes: { id: string; text?: string }[] }).nodes.find(
      (n) => n.id === "n1",
    );
    expect(node?.text).toBe("hi");

    const list = await vault.canvases.list();
    expect(list.map((c) => c.path)).toContain("Board.canvas");
  });

  it("creates a base and manages views/properties", async () => {
    await vault.bases.create("Tasks.base");
    const withView = await vault.bases.addView("Tasks.base", { name: "All", type: "table" });
    expect(withView.kind).toBe("base");

    await vault.bases.setProperty("Tasks.base", "status", { displayName: "Status" });
    const read = await vault.bases.read("Tasks.base");
    const value = read.value as { views?: unknown[]; properties?: Record<string, unknown> };
    expect(value.views?.length).toBeGreaterThan(0);
    expect(value.properties?.status).toBeTruthy();

    const list = await vault.bases.list();
    expect(list.map((b) => b.path)).toContain("Tasks.base");
  });
});
