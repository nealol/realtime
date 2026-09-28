import { describe, expect, it } from "vitest";
import { embeddedLinkpaths, resolveAttachmentLink } from "../../src/noteAttachments";

describe("embeddedLinkpaths", () => {
  it("extracts wiki and markdown embeds without aliases, sizes or subpaths", () => {
    const note = [
      "![[pic.png]] and ![[photo.jpg|300]]",
      "![[Scan.pdf#page=3]]",
      "![diagram](../Attachments/diagram.svg)",
      '![spaced](<My Images/a b.png> "title")',
      "![encoded](My%20Images/c%20d.png)",
      "![remote](https://example.com/x.png) ![data](data:image/png;base64,AAAA)",
      "[[not-an-embed.png]]",
    ].join("\n");
    expect(embeddedLinkpaths(note)).toEqual([
      "pic.png",
      "photo.jpg",
      "Scan.pdf",
      "../Attachments/diagram.svg",
      "My Images/a b.png",
      "My Images/c d.png",
    ]);
  });

  it("ignores embeds inside code", () => {
    const note = "```md\n![[in-fence.png]]\n```\nInline `![[inline.png]]` then ![[real.png]]";
    expect(embeddedLinkpaths(note)).toEqual(["real.png"]);
  });
});

describe("resolveAttachmentLink", () => {
  const candidates = [
    "Attachments/pic.png",
    "Daily/pic.png",
    "Deep/nested/folder/pic.png",
    "Attachments/diagram.svg",
    "Projects/Alpha/assets/Logo.PNG",
  ];

  it("prefers the note's own folder for a bare file name, then the shortest path", () => {
    expect(resolveAttachmentLink("pic.png", "Daily/today.md", candidates)).toBe("Daily/pic.png");
    expect(resolveAttachmentLink("pic.png", "Notes/other.md", candidates)).toBe("Daily/pic.png");
  });

  it("resolves note-relative, vault-absolute and suffix paths", () => {
    expect(resolveAttachmentLink("../Attachments/diagram.svg", "Daily/today.md", candidates)).toBe(
      "Attachments/diagram.svg",
    );
    expect(resolveAttachmentLink("/Attachments/pic.png", "Daily/today.md", candidates)).toBe(
      "Attachments/pic.png",
    );
    expect(resolveAttachmentLink("folder/pic.png", "x.md", candidates)).toBe(
      "Deep/nested/folder/pic.png",
    );
  });

  it("falls back to case-insensitive matching and reports misses", () => {
    expect(resolveAttachmentLink("logo.png", "Projects/Alpha/readme.md", candidates)).toBe(
      "Projects/Alpha/assets/Logo.PNG",
    );
    expect(resolveAttachmentLink("missing.png", "x.md", candidates)).toBeNull();
    expect(resolveAttachmentLink("../../../escape.png", "a.md", candidates)).toBeNull();
  });
});
