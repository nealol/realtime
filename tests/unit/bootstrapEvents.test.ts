import { describe, expect, it } from "vitest";
import { TFile } from "obsidian";
import { BootstrapEventLog, type BootstrapReplayStep } from "../../src/bootstrapEvents";

const file = (path: string) => new TFile(path);

function describeSteps(steps: BootstrapReplayStep[]): string[] {
  return steps.map((step) => {
    switch (step.type) {
      case "delete":
        return `delete ${step.path}`;
      case "rename":
        return `rename ${step.from} -> ${step.file.path}`;
      case "create":
        return `create ${step.file.path}`;
      case "modify":
        return `modify ${step.file.path}`;
    }
  });
}

describe("BootstrapEventLog", () => {
  it("keeps a delete that later events on other paths cannot supersede", () => {
    const log = new BootstrapEventLog();
    log.delete("a.md");
    log.modify("b.md", file("b.md"));
    expect(log.removed("a.md")).toBe(true);
    expect(describeSteps(log.plan())).toEqual(["delete a.md", "modify b.md"]);
  });

  it("publishes a rename followed by an edit as the rename", () => {
    const log = new BootstrapEventLog();
    log.rename("a.md", "b.md", file("b.md"));
    log.modify("b.md", file("b.md"));
    expect(log.removed("a.md")).toBe(true);
    expect(log.renamedFrom("b.md")).toBe("a.md");
    expect(describeSteps(log.plan())).toEqual(["rename a.md -> b.md"]);
  });

  it("composes a chain of renames into one move from the original path", () => {
    const log = new BootstrapEventLog();
    log.rename("a.md", "b.md", file("b.md"));
    log.rename("b.md", "c.md", file("c.md"));
    expect(describeSteps(log.plan())).toEqual(["rename a.md -> c.md"]);
    expect(log.removed("b.md")).toBe(false);
  });

  it("drops a file created and deleted within the window", () => {
    const log = new BootstrapEventLog();
    log.create("tmp.md", file("tmp.md"));
    log.modify("tmp.md", file("tmp.md"));
    log.delete("tmp.md");
    expect(log.isEmpty).toBe(true);
    expect(log.plan()).toEqual([]);
  });

  it("publishes a created-then-renamed file at its final path", () => {
    const log = new BootstrapEventLog();
    log.create("Untitled.md", file("Untitled.md"));
    log.rename("Untitled.md", "Named.md", file("Named.md"));
    expect(describeSteps(log.plan())).toEqual(["create Named.md"]);
  });

  it("treats a delete and re-create at one path as an edit in place", () => {
    const log = new BootstrapEventLog();
    log.delete("note.md");
    log.create("note.md", file("note.md"));
    expect(log.removed("note.md")).toBe(false);
    expect(describeSteps(log.plan())).toEqual(["modify note.md"]);
  });

  it("forgets a rename that was undone", () => {
    const log = new BootstrapEventLog();
    log.rename("a.md", "b.md", file("b.md"));
    log.rename("b.md", "a.md", file("a.md"));
    expect(log.removed("a.md")).toBe(false);
    expect(log.plan()).toEqual([]);
  });

  it("vacates a target before another file moves onto it", () => {
    const log = new BootstrapEventLog();
    log.rename("b.md", "c.md", file("c.md"));
    log.rename("a.md", "b.md", file("b.md"));
    expect(describeSteps(log.plan())).toEqual(["rename b.md -> c.md", "rename a.md -> b.md"]);
  });

  it("applies swapped paths as edits in place", () => {
    const log = new BootstrapEventLog();
    log.rename("a.md", "tmp.md", file("tmp.md"));
    log.rename("b.md", "a.md", file("a.md"));
    log.rename("tmp.md", "b.md", file("b.md"));
    expect(describeSteps(log.plan()).sort()).toEqual(["modify a.md", "modify b.md"]);
  });

  it("deletes the original path of a file renamed and then deleted", () => {
    const log = new BootstrapEventLog();
    log.rename("a.md", "b.md", file("b.md"));
    log.delete("b.md");
    expect(log.removed("a.md")).toBe(true);
    expect(describeSteps(log.plan())).toEqual(["delete a.md"]);
  });
});
