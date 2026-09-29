// An in-memory stand-in for the slice of the Obsidian plugin/app/vault API that
// src/Document.ts and src/VaultSync.ts touch. Vault mutations fire events
// synchronously, which keeps Document's `writingToDisk` echo-guard deterministic
// (it is set true around the mutation that triggers the event).

import { TFile, TAbstractFile } from "./obsidian-mock";
import { AuthClient } from "../../src/auth";

type Handler = (...args: any[]) => void;

export class FakeVault {
  configDir = ".obsidian";
  files = new Map<string, string>();
  /** Binary file contents, keyed by path (parallel to `files`). */
  binaries = new Map<string, ArrayBuffer>();
  folders = new Set<string>();
  private handlers: Record<string, Handler[]> = {};

  on(name: string, cb: Handler): { name: string; cb: Handler } {
    (this.handlers[name] ??= []).push(cb);
    return { name, cb };
  }
  off(name: string, cb: Handler): void {
    this.handlers[name] = (this.handlers[name] ?? []).filter((h) => h !== cb);
  }
  offref(ref: { name: string; cb: Handler }): void {
    this.off(ref.name, ref.cb);
  }
  handlerCount(name: string): number {
    return (this.handlers[name] ?? []).length;
  }
  private emit(name: string, ...args: any[]): void {
    for (const h of this.handlers[name] ?? []) h(...args);
  }

  getAbstractFileByPath(path: string): TAbstractFile | null {
    if (this.files.has(path) || this.binaries.has(path)) return new TFile(path);
    if (this.folders.has(path)) return new TAbstractFile(path);
    return null;
  }
  async read(file: TFile): Promise<string> {
    const text = this.files.get(file.path);
    if (text !== undefined) return text;
    const binary = this.binaries.get(file.path);
    if (binary) return new TextDecoder().decode(binary);
    return "";
  }
  async readBinary(file: TFile): Promise<ArrayBuffer> {
    const buf = this.binaries.get(file.path);
    if (!buf) throw new Error(`no binary at ${file.path}`);
    return buf;
  }
  async modifyBinary(file: TFile, data: ArrayBuffer): Promise<void> {
    this.binaries.set(file.path, data);
    this.emit("modify", new TFile(file.path));
  }
  async createBinary(path: string, data: ArrayBuffer): Promise<TFile> {
    this.binaries.set(path, data);
    const f = new TFile(path);
    this.emit("create", f);
    return f;
  }
  async modify(file: TFile, text: string): Promise<void> {
    this.files.set(file.path, text);
    this.binaries.delete(file.path);
    this.emit("modify", new TFile(file.path));
  }
  async create(path: string, text: string): Promise<TFile> {
    this.files.set(path, text);
    this.binaries.delete(path);
    const f = new TFile(path);
    this.emit("create", f);
    return f;
  }
  async delete(file: TAbstractFile): Promise<void> {
    this.files.delete(file.path);
    this.binaries.delete(file.path);
    this.emit("delete", new TFile(file.path));
  }
  async createFolder(path: string): Promise<void> {
    this.folders.add(path);
  }
  getMarkdownFiles(): TFile[] {
    return [...this.files.keys()].filter((p) => p.endsWith(".md")).map((p) => new TFile(p));
  }
  getFiles(): TFile[] {
    return [...this.files.keys(), ...this.binaries.keys()].map((p) => new TFile(p));
  }
  getName(): string {
    return "fake-vault";
  }

  /** Test helper: simulate an external/offline edit + the resulting event. */
  async rename(fileOrPath: TAbstractFile | string, newPath: string): Promise<void> {
    const oldPath = typeof fileOrPath === "string" ? fileOrPath : fileOrPath.path;
    const content = this.files.get(oldPath) ?? "";
    this.files.delete(oldPath);
    this.files.set(newPath, content);
    this.emit("rename", new TFile(newPath), oldPath);
  }
}

export interface FakePlugin {
  settings: {
    authServerUrl: string;
    sessionToken: string;
    userDisplayName: string;
    userEmail: string;
    userId: string;
    gitEmail: string;
    userPictureUrl: string;
    userAvatarUrlOverride: string;
    userAvatarUrl: string;
    activeVaultId: string;
    clientName: string;
    clientColor: string;
    clientColorLight: string;
    enabled: boolean;
    syncBinaries: boolean;
    syncCanvases: boolean;
    syncBases: boolean;
    binaryExcludeGlobs: string;
    syncConfigEnabled: boolean;
    configIncludeGlobs: string[];
    diagnosticLogging: boolean;
    mobileMaxResidentDocs: number;
    mobileRecentResidentDocs: number;
    recentPaths: string[];
  };
  auth: AuthClient;
  app: {
    vault: FakeVault;
    workspace: { on: () => unknown };
    secretStorage: {
      getSecret: (key: string) => string | null;
      setSecret: (key: string, value: string) => void;
      deleteSecret: (key: string) => void;
    };
  };
  registerEvent: (ref: unknown) => void;
  applyAwarenessTo: (doc: unknown) => void;
  setStatus: (status: string) => void;
  setUploadStatus: (status: "idle" | "uploading" | "pending") => void;
  saveSettings: () => Promise<void>;
  applyDiagnosticLoggingSetting: () => void;
}

export function makeFakePlugin(
  authServerUrl: string,
  opts: {
    sessionToken: string;
    activeVaultId: string;
    clientName?: string;
    recentPaths?: string[];
    mobileMaxResidentDocs?: number;
    mobileRecentResidentDocs?: number;
  },
): { plugin: FakePlugin; vault: FakeVault } {
  const vault = new FakeVault();
  const secrets = new Map<string, string>([["realtime-session-token", opts.sessionToken]]);
  const plugin: FakePlugin = {
    settings: {
      authServerUrl,
      sessionToken: opts.sessionToken,
      userDisplayName: "",
      userEmail: "",
      userId: "",
      gitEmail: "",
      userPictureUrl: "",
      userAvatarUrlOverride: "",
      userAvatarUrl: "",
      activeVaultId: opts.activeVaultId,
      clientName: opts.clientName ?? "Test Client",
      clientColor: "#ffffff",
      clientColorLight: "#ffffff33",
      enabled: true,
      syncBinaries: true,
      syncCanvases: true,
      syncBases: true,
      binaryExcludeGlobs: "",
      syncConfigEnabled: false,
      configIncludeGlobs: [],
      diagnosticLogging: false,
      mobileMaxResidentDocs: opts.mobileMaxResidentDocs ?? 16,
      mobileRecentResidentDocs: opts.mobileRecentResidentDocs ?? 8,
      recentPaths: opts.recentPaths ?? [],
    },
    // Set just below, once the object exists (AuthClient needs the plugin).
    auth: undefined as unknown as AuthClient,
    app: {
      vault,
      workspace: { on: () => ({}) },
      secretStorage: {
        getSecret: (key) => secrets.get(key) ?? null,
        setSecret: (key, value) => {
          secrets.set(key, value);
        },
        deleteSecret: (key) => {
          secrets.delete(key);
        },
      },
    },
    registerEvent: () => {},
    applyAwarenessTo: () => {},
    setStatus: () => {},
    setUploadStatus: () => {},
    saveSettings: async () => {},
    applyDiagnosticLoggingSetting: () => {},
  };
  plugin.auth = new AuthClient(plugin as any);
  (plugin.auth as any).supportsCapability = (name: string) => name === "documentInvalidation";
  return { plugin, vault };
}
