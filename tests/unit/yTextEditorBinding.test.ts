import { describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import { YTextEditorBinding, type TextEditorAdapter } from "../../src/editor/YTextEditorBinding";

function editor(initial: string) {
  let text = initial;
  const listeners = new Set<() => void>();
  const adapter: TextEditorAdapter & { type(value: string): void } = {
    getText: () => text,
    applyText: vi.fn((value: string) => {
      text = value;
    }),
    onChange: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    type: (value) => {
      text = value;
      listeners.forEach((listener) => listener());
    },
  };
  return adapter;
}

function ready(mayPush = true) {
  return {
    whenReady: async () => {},
    isReady: () => true,
    mayPushToShared: () => mayPush,
  };
}

describe("YTextEditorBinding", () => {
  it("applies a large remote text-card edit without replacing the binding", async () => {
    const ydoc = new Y.Doc();
    const ytext = ydoc.getText("card");
    ytext.insert(0, "start");
    const adapter = editor("start");
    const binding = new YTextEditorBinding(adapter, ytext, ready());
    await Promise.resolve();
    const large = "x".repeat(100_000);
    ydoc.transact(() => {
      ytext.delete(0, ytext.length);
      ytext.insert(0, large);
    });
    expect(adapter.getText()).toBe(large);
    binding.destroy();
  });

  it("merges concurrent character edits through Y.Text", async () => {
    const first = new Y.Doc();
    const second = new Y.Doc();
    const firstText = first.getText("text");
    firstText.insert(0, "abcd");
    Y.applyUpdate(second, Y.encodeStateAsUpdate(first));
    const secondText = second.getText("text");
    const firstEditor = editor("abcd");
    const secondEditor = editor("abcd");
    const firstBinding = new YTextEditorBinding(firstEditor, firstText, ready());
    const secondBinding = new YTextEditorBinding(secondEditor, secondText, ready());
    await Promise.resolve();
    firstEditor.type("aXbcd");
    secondEditor.type("abcdY");
    Y.applyUpdate(first, Y.encodeStateAsUpdate(second));
    Y.applyUpdate(second, Y.encodeStateAsUpdate(first));
    expect(firstText.toString()).toBe(secondText.toString());
    expect(firstText.toString()).toContain("X");
    expect(firstText.toString()).toContain("Y");
    firstBinding.destroy();
    secondBinding.destroy();
  });

  it("does not seed a protected unsynchronized empty shared document", async () => {
    const doc = new Y.Doc();
    const ytext = doc.getText("text");
    const adapter = editor("existing disk text");
    const binding = new YTextEditorBinding(adapter, ytext, ready(false));
    await Promise.resolve();
    expect(ytext.toString()).toBe("");
    expect(adapter.getText()).toBe("existing disk text");
    binding.destroy();
  });

  it("preserves text typed before a polled Canvas editor attaches", () => {
    const doc = new Y.Doc();
    const ytext = doc.getText("text");
    ytext.insert(0, "shared");
    const adapter = editor("local shared");
    adapter.hasLocalChanges = () => true;
    const binding = new YTextEditorBinding(adapter, ytext, ready());
    expect(ytext.toString()).toBe("local shared");
    expect(adapter.getText()).toBe("local shared");
    binding.destroy();
  });

  it("merges text typed before readiness with remote changes instead of overwriting them", async () => {
    const doc = new Y.Doc();
    const ytext = doc.getText("text");
    ytext.insert(0, "first\nsecond\n");
    let resolveReady!: () => void;
    const whenReady = new Promise<void>((resolve) => {
      resolveReady = resolve;
    });
    let isReady = false;
    const onConflict = vi.fn();
    const adapter = editor("first\nsecond\n");
    const binding = new YTextEditorBinding(adapter, ytext, {
      whenReady: () => whenReady,
      isReady: () => isReady,
      mayPushToShared: () => true,
      onConflict,
    });
    adapter.type("first\nsecond\nlocal\n");
    ytext.insert(0, "remote\n");
    isReady = true;
    resolveReady();
    await Promise.resolve();

    expect(ytext.toString()).toBe("remote\nfirst\nsecond\nlocal\n");
    expect(adapter.getText()).toBe("remote\nfirst\nsecond\nlocal\n");
    expect(onConflict).not.toHaveBeenCalled();
    binding.destroy();
  });

  it("pulls remote text and removes listeners on destroy", async () => {
    const doc = new Y.Doc();
    const ytext = doc.getText("text");
    ytext.insert(0, "one");
    const adapter = editor("one");
    const binding = new YTextEditorBinding(adapter, ytext, ready());
    await Promise.resolve();
    ytext.insert(3, " two");
    expect(adapter.getText()).toBe("one two");
    binding.destroy();
    ytext.insert(7, " three");
    expect(adapter.getText()).toBe("one two");
  });
});
