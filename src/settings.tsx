import {
  App,
  ExtraButtonComponent,
  Modal,
  Notice,
  PluginSettingTab,
  ToggleComponent,
} from "obsidian";
import { createRoot, type Root } from "react-dom/client";
import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import type RealtimePlugin from "./main";
import { FileDiff } from "@pierre/diffs/react";
import { parseDiffFromFile } from "@pierre/diffs";
import {
  validateAvatarUrl,
  validateGitEmail,
  type CursorAuditEntry,
  type GitBackupConfig,
  type KnownSession,
  type MemberInfo,
  type RemoteCursorInfo,
  type StorageUsage,
  type VaultInfo,
} from "./auth";
import { CompatibilityError } from "./caps";
import { PLUGIN_NAME } from "./brand";
import { generateClientIdentity } from "./names";
import { openTrashModal } from "./TrashModal";
import {
  CONFIG_CATEGORIES,
  defaultConfigSyncCategories,
  type ConfigSyncCategories,
} from "./configCategories";
import { TechnicalDetails } from "./TechnicalDetails";

export interface RealtimeSettings {
  /** Base URL of the Realtime auth server, e.g. http://127.0.0.1:8081 */
  authServerUrl: string;
  /**
   * Stable id of the server at {@link authServerUrl}, from `/api/server-info`.
   * Combined with the server host, it namespaces this account's session token
   * in Obsidian's SecretStorage (which is shared across local vaults), so one
   * Obsidian client can hold tokens for multiple servers without collisions.
   */
  authServerId: string;
  /** Identity from /api/me, cached for status + awareness defaults. */
  userId: string;
  userDisplayName: string;
  userEmail: string;
  gitEmail: string;
  /** OpenID picture URL cached from /api/me; read-only default avatar source. */
  userPictureUrl: string;
  /** User-configured avatar URL override from /api/me; empty means use userPictureUrl. */
  userAvatarUrlOverride: string;
  /** Effective avatar URL shown in presence: userAvatarUrlOverride || userPictureUrl. */
  userAvatarUrl: string;
  /** The server vault UUID currently synced into this local vault; "" if none. */
  activeVaultId: string;
  /** This client's display name (shown on remote cursors). */
  clientName: string;
  /**
   * True once the user has explicitly edited/randomized the display name.
   * Until then the name is a placeholder (the auto-generated two-word
   * identity), and login replaces it with the account's SSO display name.
   */
  clientNameCustomized: boolean;
  /** This client's cursor color. */
  clientColor: string;
  clientColorLight: string;
  /** Whether syncing is enabled. */
  enabled: boolean;
  /** Whether to sync binary (non-Markdown) files via the content-addressed blob store. */
  syncBinaries: boolean;
  /** Whether to sync Obsidian Canvas files as structured CRDT documents. */
  syncCanvases: boolean;
  /** Whether to sync Obsidian Bases files as structured CRDT documents. */
  syncBases: boolean;
  /**
   * Comma-separated globs (matched against the vault-relative path) of binary
   * files to exclude from sync, e.g. `*.tmp, .obsidian/**`. Empty syncs all.
   */
  binaryExcludeGlobs: string;
  /** Whether this device syncs its active Obsidian config folder. Per-device. */
  syncConfigEnabled: boolean;
  /**
   * Per-device vault configuration sync toggles, mirroring Obsidian Sync's
   * category list (main settings, appearance, hotkeys, plugin lists, ...).
   */
  configSyncCategories: ConfigSyncCategories;
  /** Hidden advanced setting for verbose diagnostic logging. */
  diagnosticLogging: boolean;
  /** Maximum number of per-file CRDT documents retained in memory on mobile. */
  mobileMaxResidentDocs: number;
  /** Most-recently opened documents protected from mobile hibernation. */
  mobileRecentResidentDocs: number;
  /** Recently opened syncable note/structured paths, newest first. */
  recentPaths: string[];
}

export function defaultSettings(): RealtimeSettings {
  const identity = generateClientIdentity();
  return {
    authServerUrl: "http://127.0.0.1:8081",
    authServerId: "",
    userId: "",
    userDisplayName: "",
    userEmail: "",
    gitEmail: "",
    userPictureUrl: "",
    userAvatarUrlOverride: "",
    userAvatarUrl: "",
    activeVaultId: "",
    clientName: identity.name,
    clientNameCustomized: false,
    clientColor: identity.color,
    clientColorLight: identity.colorLight,
    enabled: true,
    syncBinaries: true,
    syncCanvases: true,
    syncBases: true,
    binaryExcludeGlobs: "",
    syncConfigEnabled: false,
    configSyncCategories: defaultConfigSyncCategories(),
    diagnosticLogging: false,
    mobileMaxResidentDocs: 16,
    mobileRecentResidentDocs: 8,
    recentPaths: [],
  };
}

export class RealtimeSettingTab extends PluginSettingTab {
  plugin: RealtimePlugin;
  private root: Root | null = null;

  constructor(app: App, plugin: RealtimePlugin) {
    super(app, plugin);
    this.plugin = plugin;
  }

  display(): void {
    this.root?.unmount();
    this.root = null;
    this.containerEl.empty();
    this.root = createRoot(this.containerEl);
    this.root.render(
      <SettingsView app={this.app} plugin={this.plugin} refresh={() => this.display()} />,
    );
  }

  hide(): void {
    this.root?.unmount();
    this.root = null;
    this.containerEl.empty();
  }
}

function SettingsView({
  app,
  plugin,
  refresh,
}: {
  app: App;
  plugin: RealtimePlugin;
  refresh: () => void;
}) {
  const fullyConfigured = plugin.auth.isLoggedIn && !!plugin.settings.activeVaultId;
  return fullyConfigured ? (
    <FullSettings app={app} plugin={plugin} refresh={refresh} />
  ) : (
    <SetupView app={app} plugin={plugin} refresh={refresh} />
  );
}

/**
 * Banner shown when a server-info compatibility check failed. Never rendered
 * when compatible — per project policy, the plugin does not nudge about newer
 * server versions unless compatibility is actually broken.
 *
 * Most callers pass `plugin.lastCompatibilityError`, which `Auth.serverInfoChecked`
 * updates on every check of the active server. Non-persisted: reflects the live
 * server, not config.
 */
function CompatibilityBanner({ error: err }: { error: RealtimePlugin["lastCompatibilityError"] }) {
  if (!err) return null;
  const isClientTooOld = err.reason === "client-too-old";
  const title = isClientTooOld ? "Plugin update required" : "Server incompatible";
  const versionLine = err.serverVersion ? ` (server version ${err.serverVersion})` : "";
  return (
    <div className="realtime-warning-box" style={{ marginBottom: "16px" }}>
      <strong>{title}</strong>
      <p>
        {isClientTooOld
          ? `This server requires a newer version of the ${PLUGIN_NAME} plugin.`
          : `This server is incompatible with this version of the ${PLUGIN_NAME} plugin.`}
        {versionLine}
      </p>
      <div className="setting-item-description">{err.detail}</div>
      {isClientTooOld ? (
        <div className="setting-item-description">Update the plugin via BRAT and reload.</div>
      ) : (
        <div className="setting-item-description">
          Point the plugin at a compatible server, or ask the server operator to upgrade/downgrade
          to a matching release.
        </div>
      )}
    </div>
  );
}

type SetupStep = "server" | "choose" | "create" | "existing" | "invite";

function SetupView({
  app,
  plugin,
  refresh,
}: {
  app: App;
  plugin: RealtimePlugin;
  refresh: () => void;
}) {
  const [step, setStep] = useState<SetupStep>(plugin.auth.isLoggedIn ? "choose" : "server");
  const [serverUrl, setServerUrl] = useState(plugin.settings.authServerUrl);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [sessions, setSessions] = useState<KnownSession[]>([]);
  // Compatibility of the server typed in the setup form. Checking a candidate
  // deliberately leaves the active server's `lastCompatibilityError` alone.
  const [candidateCompat, setCandidateCompat] =
    useState<RealtimePlugin["lastCompatibilityError"]>(null);
  // Reveal the paste-code fallback shortly after SSO starts, in case the deep
  // link back into Obsidian doesn't fire.
  const showPaste = useDelayedFlag(busy, 2000);

  useEffect(() => {
    let cancelled = false;
    setSessions([]);
    if (step !== "server" || busy) return;
    const timeout = window.setTimeout(() => {
      void plugin.auth
        .validSessionsForServer(serverUrl)
        .then((result) => {
          if (cancelled) return;
          setSessions(result);
          setCandidateCompat(null);
        })
        .catch((e: unknown) => {
          if (cancelled) return;
          setSessions([]);
          setCandidateCompat(
            e instanceof CompatibilityError
              ? { reason: e.reason, detail: e.message, serverVersion: e.serverVersion }
              : null,
          );
        });
    }, 250);
    return () => {
      cancelled = true;
      window.clearTimeout(timeout);
    };
  }, [busy, plugin, serverUrl, step]);

  return (
    <div className="realtime-setup-wrap">
      <div className="realtime-setup-card">
        <h2>Set up {PLUGIN_NAME}</h2>
        <CompatibilityBanner
          error={step === "server" ? candidateCompat : plugin.lastCompatibilityError}
        />
        {step === "server" ? (
          <form
            style={{ marginTop: "-4px" }}
            onSubmit={(event) => {
              event.preventDefault();
              void (async () => {
                setBusy(true);
                setError("");
                try {
                  await plugin.auth.loginToServer(serverUrl);
                  await plugin.onLoggedIn();
                  setStep("choose");
                } catch (e) {
                  setError(e instanceof Error ? e.message : String(e));
                } finally {
                  setBusy(false);
                }
              })();
            }}
          >
            <p className="setting-item-description">
              Enter your Realtime server URL to start syncing this vault.
            </p>
            <input
              className="realtime-modal-input"
              type="text"
              placeholder="https://realtime.example.com"
              value={serverUrl}
              onChange={(event) => setServerUrl(event.currentTarget.value)}
            />
            {error ? <p className="realtime-error">{error}</p> : null}
            <SsoPasteFallback plugin={plugin} visible={showPaste} />
            <button className="mod-cta realtime-wide-button" type="submit" disabled={busy}>
              {busy ? "Waiting for SSO..." : "Log in with SSO"}
            </button>
            {sessions.map((session) => (
              <button
                key={session.tokenKey}
                className="realtime-wide-button"
                type="button"
                disabled={busy}
                onClick={() =>
                  void (async () => {
                    setBusy(true);
                    setError("");
                    try {
                      await plugin.auth.useKnownSession(session);
                      await plugin.onLoggedIn();
                      setStep("choose");
                    } catch (e) {
                      setError(e instanceof Error ? e.message : String(e));
                    } finally {
                      setBusy(false);
                    }
                  })()
                }
              >
                Log in as {session.displayName || session.email}
              </button>
            ))}
          </form>
        ) : null}
        {step === "choose" ? (
          <SetupChoices
            onCreate={() => setStep("create")}
            onExisting={() => setStep("existing")}
            onInvite={() => setStep("invite")}
          />
        ) : null}
        {step === "create" ? (
          <CreateVaultStep
            app={app}
            plugin={plugin}
            refresh={refresh}
            onBack={() => setStep("choose")}
          />
        ) : null}
        {step === "existing" ? (
          <ExistingVaultStep plugin={plugin} refresh={refresh} onBack={() => setStep("choose")} />
        ) : null}
        {step === "invite" ? (
          <InviteVaultStep plugin={plugin} refresh={refresh} onBack={() => setStep("choose")} />
        ) : null}
      </div>
    </div>
  );
}

function SetupChoices({
  onCreate,
  onExisting,
  onInvite,
}: {
  onCreate: () => void;
  onExisting: () => void;
  onInvite: () => void;
}) {
  return (
    <div className="realtime-choice-list" style={{ marginTop: "-16px" }}>
      <p className="setting-item-description">
        Choose how this local vault should connect to a remote vault.
      </p>
      <button className="realtime-choice" onClick={onCreate}>
        <strong>Create a new Remote Vault</strong>
        <span>Use the Markdown files already in this local vault.</span>
      </button>
      <button className="realtime-choice" onClick={onExisting}>
        <strong>Initialize an existing Remote Vault</strong>
        <span>Replace local vault with a synced remote one.</span>
      </button>
      <button className="realtime-choice" onClick={onInvite}>
        <strong>Join a new Remote Vault</strong>
        <span>Use an invite code to join a vault.</span>
      </button>
    </div>
  );
}

function CreateVaultStep({
  app,
  plugin,
  refresh,
  onBack,
}: {
  app: App;
  plugin: RealtimePlugin;
  refresh: () => void;
  onBack: () => void;
}) {
  const [name, setName] = useState(app.vault.getName());
  const [busy, setBusy] = useState(false);
  return (
    <>
      {/*<h3>Create Remote Vault</h3>*/}
      <p className="setting-item-description">Name your new remote vault.</p>
      <input
        className="realtime-modal-input"
        type="text"
        value={name}
        onChange={(event) => setName(event.currentTarget.value)}
      />
      <div className="realtime-actions">
        <button onClick={onBack}>Back</button>
        <button
          className="mod-cta"
          disabled={busy || !name.trim()}
          onClick={() =>
            void runNotice(setBusy, async () => {
              await plugin.createAndActivateVault(name.trim());
              new Notice(`${PLUGIN_NAME}: created and syncing "${name.trim()}".`);
              refresh();
            })
          }
        >
          Create & Sync
        </button>
      </div>
    </>
  );
}

function ExistingVaultStep({
  plugin,
  refresh,
  onBack,
}: {
  plugin: RealtimePlugin;
  refresh: () => void;
  onBack: () => void;
}) {
  const { vaults, error, reload } = useVaults(plugin);
  const [confirm, setConfirm] = useState<VaultInfo | null>(null);
  return (
    <>
      {/*<h3>Initialize Existing Remote Vault</h3>*/}
      {confirm ? (
        <EraseConfirm
          vault={confirm}
          onCancel={() => setConfirm(null)}
          onConfirm={() =>
            void runNotice(undefined, async () => {
              await plugin.adoptVault(confirm.id, confirm.name);
              refresh();
            })
          }
        />
      ) : null}
      {!confirm ? (
        <>
          <p className="setting-item-description">Choose a remote vault to clone.</p>
          {error ? <p className="realtime-error">{error}</p> : null}
          {!error && vaults === null ? (
            <p className="setting-item-description">Loading vaults...</p>
          ) : null}
          {vaults?.length === 0 ? (
            <p className="setting-item-description">No remote vaults found.</p>
          ) : null}
          <div className="realtime-choice-list">
            {vaults?.map((vault) => (
              <button key={vault.id} className="realtime-choice" onClick={() => setConfirm(vault)}>
                <strong>{vault.name}</strong>
                <span>
                  Role: {vault.role}
                  {vault.owner ? " (owner)" : ""}
                </span>
              </button>
            ))}
          </div>
          <div className="realtime-actions">
            <button onClick={onBack}>Back</button>
            <button onClick={reload}>Refresh</button>
          </div>
        </>
      ) : null}
    </>
  );
}

function InviteVaultStep({
  plugin,
  refresh,
  onBack,
}: {
  plugin: RealtimePlugin;
  refresh: () => void;
  onBack: () => void;
}) {
  const [code, setCode] = useState("");
  const [joined, setJoined] = useState<{ vaultId: string; name: string } | null>(null);
  const [busy, setBusy] = useState(false);
  return (
    <>
      {/*<h3>Join Remote Vault</h3>*/}
      {joined ? (
        <EraseConfirm
          vault={{ id: joined.vaultId, name: joined.name }}
          onCancel={onBack}
          onConfirm={() =>
            void runNotice(undefined, async () => {
              await plugin.adoptVault(joined.vaultId, joined.name);
              refresh();
            })
          }
        />
      ) : (
        <>
          <p className="setting-item-description">Enter an invite code from another person.</p>
          <input
            className="realtime-modal-input"
            type="text"
            placeholder="four-word-invite-code"
            value={code}
            onChange={(event) => setCode(event.currentTarget.value.trim())}
          />
          <div className="realtime-actions">
            <button onClick={onBack}>Back</button>
            <button
              className="mod-cta"
              disabled={busy || !code}
              onClick={() =>
                void runNotice(setBusy, async () => {
                  setJoined(await plugin.auth.redeemInvite(code));
                })
              }
            >
              Redeem Invite
            </button>
          </div>
        </>
      )}
    </>
  );
}

function EraseConfirm({
  vault,
  onConfirm,
  onCancel,
}: {
  vault: Pick<VaultInfo, "id" | "name">;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="realtime-warning-box">
      <strong>Erase local Markdown and sync "{vault.name}"?</strong>
      <p>
        This deletes all non-conflict-copy Markdown files in this local Obsidian vault and replaces
        them with the remote vault.
      </p>
      <div className="realtime-actions">
        <button onClick={onCancel}>Cancel</button>
        <button className="mod-warning" onClick={onConfirm}>
          Erase & Sync
        </button>
      </div>
    </div>
  );
}

function FullSettings({
  app,
  plugin,
  refresh,
}: {
  app: App;
  plugin: RealtimePlugin;
  refresh: () => void;
}) {
  return (
    <>
      {/*<h2>Realtime</h2>*/}
      <CompatibilityBanner error={plugin.lastCompatibilityError} />
      <AccountSection plugin={plugin} refresh={refresh} />
      <EnableSyncSection plugin={plugin} refresh={refresh} />
      <StructuredSyncSection plugin={plugin} refresh={refresh} />
      <BinarySyncSection plugin={plugin} refresh={refresh} />
      <DeviceSection plugin={plugin} />
      <VaultDetails app={app} plugin={plugin} />
      <AdvancedSettings app={app} plugin={plugin} refresh={refresh} />
      <SettingsTechnicalDetails plugin={plugin} />
    </>
  );
}

function SettingsTechnicalDetails({ plugin }: { plugin: RealtimePlugin }) {
  const [serverVersion, setServerVersion] = useState("Checking…");
  useEffect(() => {
    let cancelled = false;
    void plugin.auth
      .serverInfoChecked(plugin.settings.authServerUrl)
      .then((info) => {
        if (!cancelled) setServerVersion(info.version ?? "Not reported");
      })
      .catch(() => {
        if (!cancelled) {
          setServerVersion(plugin.lastCompatibilityError?.serverVersion ?? "Unavailable");
        }
      });
    return () => {
      cancelled = true;
    };
  }, [plugin, plugin.settings.authServerUrl]);

  return (
    <TechnicalDetails
      clientVersion={plugin.manifest.version}
      serverVersion={serverVersion}
      vaultId={plugin.settings.activeVaultId}
    />
  );
}

function initials(name: string): string {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return "?";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

function AccountSection({ plugin, refresh }: { plugin: RealtimePlugin; refresh: () => void }) {
  const [gitEmail, setGitEmail] = useState(plugin.settings.gitEmail || "");
  const [emailError, setEmailError] = useState("");
  const [avatarOverride, setAvatarOverride] = useState(plugin.settings.userAvatarUrlOverride || "");
  const [avatarError, setAvatarError] = useState("");
  return (
    <>
      <h3>Account</h3>
      <SettingRow
        name={plugin.settings.userDisplayName || "Signed in"}
        desc={plugin.settings.userEmail}
        control={
          <button
            className="mod-warning"
            onClick={() =>
              void runNotice(undefined, async () => {
                await plugin.logout();
                refresh();
              })
            }
          >
            Log out
          </button>
        }
      />
      {plugin.settings.userAvatarUrl ? (
        <img className="realtime-account-avatar" src={plugin.settings.userAvatarUrl} alt="" />
      ) : (
        <div className="realtime-account-avatar is-fallback">
          {initials(plugin.settings.userDisplayName || plugin.settings.userEmail)}
        </div>
      )}
      <SettingRow
        name="Avatar URL"
        desc="Optional image URL shown to collaborators. Leave blank to use your OpenID profile picture."
        control={
          <input
            className="realtime-modal-input"
            type="url"
            value={avatarOverride}
            onChange={(event) => {
              setAvatarOverride(event.currentTarget.value);
              if (avatarError) setAvatarError("");
            }}
            onBlur={() => {
              const trimmed = avatarOverride.trim();
              if (trimmed === (plugin.settings.userAvatarUrlOverride || "")) {
                if (avatarError) setAvatarError("");
                return;
              }
              const msg = validateAvatarUrl(trimmed);
              if (msg) {
                setAvatarError(msg);
                return;
              }
              setAvatarError("");
              void runNotice(undefined, async () => {
                await plugin.auth.updateMe({ avatarUrlOverride: trimmed || null });
                plugin.updateLocalAwareness();
                refresh();
                new Notice(`${PLUGIN_NAME}: Avatar updated.`);
              });
            }}
          />
        }
      />
      {avatarError ? <p className="realtime-error">{avatarError}</p> : null}
      <SettingRow
        name="Git author email"
        desc="Optional email used as the Git author for your edits. Leave blank to use your login email."
        control={
          <input
            className="realtime-modal-input"
            type="email"
            value={gitEmail}
            placeholder={plugin.settings.userEmail}
            onChange={(event) => {
              setGitEmail(event.currentTarget.value);
              if (emailError) setEmailError("");
            }}
            onBlur={() => {
              const trimmed = gitEmail.trim();
              if (trimmed === plugin.settings.gitEmail) {
                if (emailError) setEmailError("");
                return;
              }
              const msg = validateGitEmail(trimmed);
              if (msg) {
                setEmailError(msg);
                return;
              }
              setEmailError("");
              void runNotice(undefined, async () => {
                await plugin.auth.updateMe({ gitEmail: trimmed || null });
                setGitEmail(trimmed);
                new Notice(`${PLUGIN_NAME}: Git author email updated.`);
              });
            }}
          />
        }
      />
      {emailError ? <p className="realtime-error">{emailError}</p> : null}
    </>
  );
}

function EnableSyncSection({ plugin, refresh }: { plugin: RealtimePlugin; refresh: () => void }) {
  return (
    <SettingRow
      name="Enable syncing"
      desc="When on, this vault syncs online. Turn off to stay signed in but pause online sync."
      control={
        <Toggle
          value={plugin.settings.enabled}
          onChange={(value) =>
            void runNotice(undefined, async () => {
              plugin.settings.enabled = value;
              await plugin.saveSettings();
              await plugin.reloadSync();
              if (!value) new Notice(`${PLUGIN_NAME}: syncing disabled for this vault.`);
              refresh();
            })
          }
        />
      }
    />
  );
}

function StructuredSyncSection({
  plugin,
  refresh,
}: {
  plugin: RealtimePlugin;
  refresh: () => void;
}) {
  return (
    <>
      <SettingRow
        name="Sync canvases"
        desc="Sync .canvas files as structured CRDT documents. Live canvas updates use Obsidian's private canvas API when available, with disk write-through fallback."
        control={
          <Toggle
            value={plugin.settings.syncCanvases}
            onChange={(value) =>
              void runNotice(undefined, async () => {
                plugin.settings.syncCanvases = value;
                await plugin.saveSettings();
                await plugin.reloadSync();
                refresh();
              })
            }
          />
        }
      />
      <SettingRow
        name="Sync bases"
        desc="Sync .base files as structured YAML CRDT documents. YAML formatting may be normalized when remote changes write back to disk."
        control={
          <Toggle
            value={plugin.settings.syncBases}
            onChange={(value) =>
              void runNotice(undefined, async () => {
                plugin.settings.syncBases = value;
                await plugin.saveSettings();
                await plugin.reloadSync();
                refresh();
              })
            }
          />
        }
      />
    </>
  );
}

function BinarySyncSection({ plugin, refresh }: { plugin: RealtimePlugin; refresh: () => void }) {
  const [globs, setGlobs] = useState(plugin.settings.binaryExcludeGlobs);
  return (
    <>
      <SettingRow
        name="Sync attachments"
        desc="Sync binary files (images, PDFs, and other non-Markdown files) via the content-addressed blob store."
        control={
          <Toggle
            value={plugin.settings.syncBinaries}
            onChange={(value) =>
              void runNotice(undefined, async () => {
                plugin.settings.syncBinaries = value;
                await plugin.saveSettings();
                await plugin.reloadSync();
                refresh();
              })
            }
          />
        }
      />
      {plugin.settings.syncBinaries ? (
        <SettingRow
          name="Attachment exclusions"
          desc="Comma-separated globs (matched on the file path) to skip, e.g. *.tmp, private/**."
          control={
            <input
              className="realtime-modal-input"
              type="text"
              value={globs}
              placeholder="*.tmp, private/**"
              onChange={(e) => setGlobs(e.currentTarget.value)}
              onBlur={() =>
                void (async () => {
                  if (globs === plugin.settings.binaryExcludeGlobs) return;
                  plugin.settings.binaryExcludeGlobs = globs;
                  await plugin.saveSettings();
                  await plugin.reloadSync();
                })()
              }
            />
          }
        />
      ) : null}
    </>
  );
}

/**
 * Real Obsidian `ToggleComponent` mounted into a React-managed element, so the
 * switch looks and behaves exactly like a native settings toggle. The value is
 * applied before the change handler is wired so seeding it can't re-fire it; a
 * full settings re-render remounts this, so no separate value-sync is needed.
 */
function Toggle({ value, onChange }: { value: boolean; onChange: (value: boolean) => void }) {
  const ref = useRef<HTMLSpanElement>(null);
  const onChangeRef = useRef(onChange);
  onChangeRef.current = onChange;
  const initial = useRef(value).current;
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    new ToggleComponent(el).setValue(initial).onChange((v) => onChangeRef.current(v));
    return () => {
      el.empty();
    };
  }, [initial]);
  return <span ref={ref} />;
}

/**
 * Real Obsidian `ExtraButtonComponent` mounted into a React-managed element,
 * giving the muted icon-button display used elsewhere in settings. `onClick` is
 * read through a ref so a re-render never needs to rebuild the component.
 */
function ExtraButton({
  icon,
  tooltip,
  className,
  onClick,
}: {
  icon: string;
  tooltip?: string;
  className?: string;
  onClick: () => void;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const onClickRef = useRef(onClick);
  onClickRef.current = onClick;
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const button = new ExtraButtonComponent(el).setIcon(icon).onClick(() => onClickRef.current());
    if (tooltip) button.setTooltip(tooltip);
    return () => {
      el.empty();
    };
  }, [icon, tooltip]);
  return <span className={className} ref={ref} />;
}

function DeviceSection({ plugin }: { plugin: RealtimePlugin }) {
  // Local state so randomizing/editing re-renders only this section, instead of
  // calling the page-level refresh (which reloads the slow vaults/members lists).
  const [name, setName] = useState(plugin.settings.clientName);
  const [color, setColor] = useState(plugin.settings.clientColor);

  const applyName = (value: string) => {
    setName(value);
    void runNotice(undefined, async () => {
      plugin.settings.clientName = value;
      plugin.settings.clientNameCustomized = true;
      await plugin.saveSettings();
      plugin.updateLocalAwareness();
    });
  };
  const applyColor = (value: string) => {
    setColor(value);
    void runNotice(undefined, async () => {
      plugin.settings.clientColor = value;
      plugin.settings.clientColorLight = value + "33";
      await plugin.saveSettings();
      plugin.updateLocalAwareness();
    });
  };

  return (
    <>
      <h3>Your Device</h3>
      <SettingRow
        name="Display name"
        desc="Name shown to other editors on your cursor."
        control={
          <>
            <input
              type="text"
              value={name}
              onChange={(event) => applyName(event.currentTarget.value)}
            />
            <ExtraButton
              icon="dice"
              tooltip="Randomize display name"
              onClick={() => applyName(generateClientIdentity().name)}
            />
          </>
        }
      />
      <SettingRow
        name="Cursor color"
        desc="The color of your cursor and selection for other editors."
        control={
          <>
            <input
              type="color"
              value={color}
              onChange={(event) => applyColor(event.currentTarget.value)}
            />
            <ExtraButton
              icon="dice"
              tooltip="Randomize cursor color"
              onClick={() => applyColor(generateClientIdentity().color)}
            />
          </>
        }
      />
    </>
  );
}

function VaultDetails({ plugin }: { app: App; plugin: RealtimePlugin }) {
  const { vaults, error: vaultError, reload: reloadVaults } = useVaults(plugin);
  const activeVault = useMemo(
    () => vaults?.find((vault) => vault.id === plugin.settings.activeVaultId) ?? null,
    [plugin.settings.activeVaultId, vaults],
  );
  const {
    members,
    error: membersError,
    reload: reloadMembers,
  } = useMembers(plugin, activeVault?.id ?? "");
  const reloadAll = () => {
    reloadVaults();
    reloadMembers();
  };
  return (
    <>
      <h3>Vault Details{activeVault ? ` - ${activeVault.name}` : ""}</h3>
      {vaultError ? <p className="realtime-error">{vaultError}</p> : null}
      {!activeVault ? <p className="setting-item-description">Loading vault details...</p> : null}
      {membersError ? <p className="realtime-error">{membersError}</p> : null}
      {members === null ? <p className="setting-item-description">Loading members...</p> : null}
      {activeVault &&
        members?.map((member) => (
          <MemberRow
            key={member.userId}
            plugin={plugin}
            vault={activeVault}
            member={member}
            reload={reloadAll}
          />
        ))}
      {activeVault?.role === "admin" ? (
        <InviteGenerator plugin={plugin} vault={activeVault} />
      ) : null}
      {activeVault?.role === "admin" ? (
        <StorageSection plugin={plugin} vault={activeVault} />
      ) : null}
      {activeVault?.role === "admin" ? <RemoteCursors plugin={plugin} vault={activeVault} /> : null}
      {activeVault?.role === "admin" ? (
        <GitBackupSection plugin={plugin} vault={activeVault} />
      ) : null}
    </>
  );
}

function MemberRow({
  plugin,
  vault,
  member,
  reload,
}: {
  plugin: RealtimePlugin;
  vault: VaultInfo;
  member: MemberInfo;
  reload: () => void;
}) {
  const isSelf = member.email === plugin.settings.userEmail;
  const canPromote = vault.role === "admin" && member.role !== "admin";
  const canRemove =
    !member.owner && !isSelf && (member.role === "member" ? vault.role === "admin" : !!vault.owner);
  return (
    <SettingRow
      name={`${member.displayName || member.email}${member.owner ? " (owner)" : ""}`}
      desc={`${member.email} (${member.role})`}
      control={
        <>
          {canPromote ? (
            <button
              onClick={() =>
                void runNotice(undefined, async () => {
                  await plugin.auth.promoteMember(vault.id, member.userId);
                  new Notice(`${PLUGIN_NAME}: promoted.`);
                  reload();
                })
              }
            >
              Promote to admin
            </button>
          ) : null}
          {canRemove ? (
            <button
              className="mod-warning"
              onClick={() =>
                void runNotice(undefined, async () => {
                  await plugin.auth.removeMember(vault.id, member.userId);
                  new Notice(`${PLUGIN_NAME}: member removed.`);
                  reload();
                })
              }
            >
              Remove
            </button>
          ) : null}
        </>
      }
    />
  );
}

function InviteGenerator({ plugin, vault }: { plugin: RealtimePlugin; vault: VaultInfo }) {
  const [code, setCode] = useState("");
  return (
    <SettingRow
      name="Add members"
      desc="Generate a single-use invite for this vault."
      control={
        <>
          <button
            onClick={() =>
              void runNotice(undefined, async () => {
                const invite = await plugin.auth.createInvite(vault.id);
                setCode(invite.code);
                void navigator.clipboard?.writeText(invite.code).catch(() => {});
                new Notice(`${PLUGIN_NAME} invite copied: ${invite.code}`, 15000);
              })
            }
          >
            Generate invite
          </button>
          {code ? <code>{code}</code> : null}
        </>
      }
    />
  );
}

const STORAGE_SEGMENTS = [
  { key: "current", label: "Current attachments", className: "realtime-storage-current" },
  { key: "previous", label: "Previous versions", className: "realtime-storage-previous" },
  { key: "vault", label: "Plain vault", className: "realtime-storage-vault" },
] as const;

function StorageSection({ plugin, vault }: { plugin: RealtimePlugin; vault: VaultInfo }) {
  const { usage, error, reload } = useStorageUsage(plugin, vault.id);
  const [cleaning, setCleaning] = useState(false);
  const [busy, setBusy] = useState(false);
  const [minMb, setMinMb] = useState("1");

  const sizes = usage
    ? {
        current: usage.blobsCurrentBytes,
        previous: usage.blobsPreviousBytes,
        vault: usage.plainVaultBytes ?? 0,
      }
    : { current: 0, previous: 0, vault: 0 };
  const total = sizes.current + sizes.previous + sizes.vault;

  return (
    <>
      <h3>Storage Management</h3>
      {error ? <p className="realtime-error">{error}</p> : null}
      {!usage && !error ? (
        <p className="setting-item-description">Loading storage usage...</p>
      ) : null}
      {usage ? (
        <>
          <div className="realtime-storage-bar">
            {STORAGE_SEGMENTS.map((segment) => {
              const value = sizes[segment.key];
              if (total === 0 || value === 0) return null;
              return (
                <div
                  key={segment.key}
                  className={`realtime-storage-seg ${segment.className}`}
                  style={{ width: `${(value / total) * 100}%` }}
                  title={`${segment.label}: ${formatBytes(value)}`}
                />
              );
            })}
            {total === 0 ? (
              <div
                className="realtime-storage-seg realtime-storage-empty"
                style={{ width: "100%" }}
              />
            ) : null}
          </div>
          <div className="realtime-storage-legend">
            {STORAGE_SEGMENTS.map((segment) => (
              <div key={segment.key} className="realtime-storage-legend-item">
                <span className={`realtime-storage-dot ${segment.className}`} />
                <span>{segment.label}</span>
                <span className="realtime-storage-bytes">
                  {segment.key === "vault" && usage.plainVaultBytes === null
                    ? "unavailable"
                    : formatBytes(sizes[segment.key])}
                </span>
              </div>
            ))}
            <div className="realtime-storage-legend-item realtime-storage-total">
              <span>Total</span>
              <span className="realtime-storage-bytes">{formatBytes(total)}</span>
            </div>
          </div>
        </>
      ) : null}
      <SettingRow
        name="Trash"
        desc="View and restore deleted notes, canvases, bases, and attachments."
        control={<button onClick={() => openTrashModal(plugin)}>Open trash</button>}
      />
      <SettingRow
        name="Clean up previous versions"
        desc={`Reclaim ${usage ? formatBytes(usage.blobsPreviousBytes) : "space used by"} orphaned attachment versions${usage ? ` (${usage.previousBlobCount} blob${usage.previousBlobCount === 1 ? "" : "s"})` : ""}.`}
        control={
          <button className="mod-warning" onClick={() => setCleaning((v) => !v)}>
            {cleaning ? "Hide" : "Clean up..."}
          </button>
        }
      />
      {cleaning ? (
        <div className="realtime-warning-box">
          <strong>This permanently deletes previous and deleted attachment versions.</strong>
          <p>
            Only the current versions of your attachments are kept. Older versions and the contents
            of trashed/deleted attachments are removed and can no longer be restored, including from
            any point-in-time backup that relied on them. This does not affect notes.
          </p>
          <div className="realtime-actions">
            <label className="realtime-storage-threshold">
              Only blobs ≥
              <input
                type="number"
                min="0"
                step="0.1"
                className="realtime-modal-input"
                value={minMb}
                onChange={(event) => setMinMb(event.currentTarget.value)}
              />{" "}
              MB
            </label>
            <button onClick={() => setCleaning(false)}>Cancel</button>
            <button
              className="mod-warning"
              disabled={busy}
              onClick={() =>
                void runNotice(setBusy, async () => {
                  const minBytes = Math.max(0, Math.round((parseFloat(minMb) || 0) * 1024 * 1024));
                  const result = await plugin.auth.gcBlobs(vault.id, minBytes);
                  new Notice(
                    `${PLUGIN_NAME}: removed ${result.removed} blob${result.removed === 1 ? "" : "s"}, freed ${formatBytes(result.freedBytes)}.`,
                  );
                  setCleaning(false);
                  reload();
                })
              }
            >
              Delete previous versions
            </button>
          </div>
        </div>
      ) : null}
    </>
  );
}

function useStorageUsage(plugin: RealtimePlugin, vaultId: string) {
  const [reloadKey, setReloadKey] = useState(0);
  const [usage, setUsage] = useState<StorageUsage | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!vaultId) return;
    let cancelled = false;
    setUsage(null);
    setError("");
    void (async () => {
      try {
        const result = await plugin.auth.getStorageUsage(vaultId);
        if (!cancelled) setUsage(result);
      } catch (e) {
        if (!cancelled)
          setError(`Could not load storage usage: ${e instanceof Error ? e.message : String(e)}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [plugin, vaultId, reloadKey]);
  return { usage, error, reload: () => setReloadKey((key) => key + 1) };
}

function formatBytes(bytes: number): string {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const exp = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const value = bytes / Math.pow(1024, exp);
  return `${value >= 100 || exp === 0 ? Math.round(value) : value.toFixed(1)} ${units[exp]}`;
}

function RemoteCursors({ plugin, vault }: { plugin: RealtimePlugin; vault: VaultInfo }) {
  const { cursors, error, reload } = useRemoteCursors(plugin, vault.id);
  return (
    <>
      <h3>Remote Cursors</h3>
      {error ? <p className="realtime-error">{error}</p> : null}
      {!error && cursors === null ? (
        <p className="setting-item-description">Loading remote cursors...</p>
      ) : null}
      {cursors?.length === 0 ? (
        <p className="setting-item-description">No remote cursors yet.</p>
      ) : null}
      {cursors?.map((cursor) => (
        <RemoteCursorRow
          key={cursor.id}
          plugin={plugin}
          vault={vault}
          cursor={cursor}
          reload={reload}
        />
      ))}
      <SettingRow
        name="Add remote cursor"
        desc="Create an app-specific MCP URL with OAuth support and a secret token for direct bearer API calls."
        control={
          <button
            onClick={() => new RemoteCursorNameModal(plugin.app, plugin, vault, reload).open()}
          >
            Add remote cursor
          </button>
        }
      />
    </>
  );
}

function RemoteCursorRow({
  plugin,
  vault,
  cursor,
  reload,
}: {
  plugin: RealtimePlugin;
  vault: VaultInfo;
  cursor: RemoteCursorInfo;
  reload: () => void;
}) {
  const desc = cursor.pluginId ? (
    <>
      Managed by plugin: <code>{cursor.pluginId}</code>
      <br />
      {cursor.mcpUrl}
    </>
  ) : (
    cursor.mcpUrl
  );
  return (
    <SettingRow
      name={
        <span className="realtime-row-title-action">
          {cursor.name}
          <ExtraButton
            icon="pencil"
            tooltip="Edit remote cursor"
            onClick={() =>
              new RemoteCursorNameModal(plugin.app, plugin, vault, reload, cursor).open()
            }
          />
        </span>
      }
      desc={desc}
      control={
        <>
          <button onClick={() => void copyText(cursor.mcpUrl, `${PLUGIN_NAME}: MCP URL copied.`)}>
            Copy MCP URL
          </button>
          {!cursor.pluginId ? (
            <button
              onClick={() =>
                void runNotice(undefined, async () => {
                  if (
                    !confirm(
                      `Regenerate the secret token for "${cursor.name}"? The previous token will stop working.`,
                    )
                  )
                    return;
                  const result = await plugin.auth.regenerateCursorToken(vault.id, cursor.id);
                  await copyText(result.secretToken, `${PLUGIN_NAME}: new secret token copied.`);
                })
              }
            >
              Regen API Secret
            </button>
          ) : null}
          <ExtraButton
            icon="history"
            tooltip="Audit log"
            onClick={() => new CursorAuditModal(plugin.app, plugin, vault, cursor).open()}
          />
          <ExtraButton
            className="realtime-danger-icon"
            icon="trash-2"
            tooltip="Remove remote cursor"
            onClick={() =>
              void runNotice(undefined, async () => {
                if (!confirm(`Remove remote cursor "${cursor.name}"?`)) return;
                await plugin.auth.deleteCursor(vault.id, cursor.id);
                new Notice(`${PLUGIN_NAME}: remote cursor removed.`);
                reload();
              })
            }
          />
        </>
      }
    />
  );
}

const AUDIT_OPERATION_LABELS: Record<string, string> = {
  note_create: "Created note",
  note_replace: "Replaced note",
  note_patch: "Patched note",
  note_replace_body: "Replaced note body",
  note_frontmatter: "Edited frontmatter",
  note_periodic_append: "Appended to periodic note",
  note_move: "Moved note",
  note_delete: "Deleted note",
  stream: "Streamed into note",
  structured_set: "Edited document",
  structured_create: "Created document",
  structured_move: "Moved document",
  structured_delete: "Deleted document",
  attachment_upload: "Uploaded attachment",
  attachment_delete: "Deleted attachment",
  attachment_move: "Moved attachment",
};

class CursorAuditModal extends Modal {
  private plugin: RealtimePlugin;
  private vault: VaultInfo;
  private cursor: RemoteCursorInfo;
  private root: Root | null = null;

  constructor(app: App, plugin: RealtimePlugin, vault: VaultInfo, cursor: RemoteCursorInfo) {
    super(app);
    this.plugin = plugin;
    this.vault = vault;
    this.cursor = cursor;
  }

  onOpen(): void {
    this.modalEl.addClass("realtime-audit-modal");
    this.root = createRoot(this.contentEl);
    this.root.render(
      <CursorAuditView plugin={this.plugin} vault={this.vault} cursor={this.cursor} />,
    );
  }

  onClose(): void {
    this.root?.unmount();
    this.root = null;
    this.contentEl.empty();
  }
}

function CursorAuditView({
  plugin,
  vault,
  cursor,
}: {
  plugin: RealtimePlugin;
  vault: VaultInfo;
  cursor: RemoteCursorInfo;
}) {
  const [entries, setEntries] = useState<CursorAuditEntry[] | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const load = async (before?: number) => {
    setError("");
    try {
      const page = await plugin.auth.listCursorAudit(vault.id, cursor.id, before);
      setEntries((current) =>
        before === undefined ? page.entries : [...(current ?? []), ...page.entries],
      );
      setHasMore(page.hasMore);
    } catch (e) {
      setError(`Could not load audit log: ${e instanceof Error ? e.message : String(e)}`);
    }
  };
  useEffect(() => {
    void load();
  }, [plugin, vault.id, cursor.id]);

  return (
    <>
      <h3>Audit log: {cursor.name}</h3>
      <p className="setting-item-description">
        Operations this remote cursor performed via the API, MCP or streaming. Entries are kept for
        3 days; undo applies the inverse change as you.
      </p>
      {error ? <p className="realtime-error">{error}</p> : null}
      {entries === null && !error ? (
        <p className="setting-item-description">Loading audit log...</p>
      ) : null}
      {entries?.length === 0 ? (
        <p className="setting-item-description">No operations recorded in the last 3 days.</p>
      ) : null}
      <div className="realtime-audit-entries">
        {entries?.map((entry) => (
          <AuditEntryRow
            key={entry.id}
            plugin={plugin}
            vault={vault}
            cursor={cursor}
            entry={entry}
            busy={busy}
            setBusy={setBusy}
            reload={() => void load()}
          />
        ))}
      </div>
      {hasMore && entries?.length ? (
        <div className="realtime-actions">
          <button onClick={() => void load(entries[entries.length - 1].createdAt)}>
            Load more
          </button>
        </div>
      ) : null}
    </>
  );
}

function AuditEntryRow({
  plugin,
  vault,
  cursor,
  entry,
  busy,
  setBusy,
  reload,
}: {
  plugin: RealtimePlugin;
  vault: VaultInfo;
  cursor: RemoteCursorInfo;
  entry: CursorAuditEntry;
  busy: boolean;
  setBusy: (busy: boolean) => void;
  reload: () => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const truncated = entry.details?.truncated === true;
  const hasDiff = entry.beforeContent != null || entry.afterContent != null;
  const label = AUDIT_OPERATION_LABELS[entry.operation] ?? entry.operation;
  const pathText = entry.toPath ? `${entry.path} → ${entry.toPath}` : entry.path;

  const undo = () =>
    void runNotice(setBusy, async () => {
      try {
        await plugin.auth.undoCursorAudit(vault.id, cursor.id, entry.id);
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e);
        if (message === "changed_since") {
          if (
            !confirm(
              "The document changed after this operation. Undo anyway, overwriting the newer content?",
            )
          )
            return;
          await plugin.auth.undoCursorAudit(vault.id, cursor.id, entry.id, true);
        } else if (message === "missing") {
          throw new Error("Cannot undo: the document no longer exists.");
        } else {
          throw e;
        }
      }
      new Notice(`${PLUGIN_NAME}: operation undone.`);
      reload();
    });

  return (
    <div className="realtime-audit-entry">
      <div className="realtime-audit-entry-row">
        <div className="realtime-audit-entry-info">
          <span className="realtime-audit-entry-op">{label}</span>
          {entry.undoneAt ? <span className="realtime-audit-undone">Undone</span> : null}
          <div className="setting-item-description">
            {pathText} · {new Date(entry.createdAt).toLocaleString()}
          </div>
        </div>
        <div className="realtime-audit-entry-actions">
          {hasDiff ? (
            <button onClick={() => setExpanded((value) => !value)}>
              {expanded ? "Hide diff" : "Show diff"}
            </button>
          ) : null}
          {!entry.undoneAt && !truncated ? (
            <button disabled={busy} onClick={undo}>
              Undo
            </button>
          ) : null}
        </div>
      </div>
      {truncated ? (
        <p className="setting-item-description">
          Content was too large to store fully; undo is unavailable.
        </p>
      ) : null}
      {expanded && hasDiff ? (
        <div className="realtime-audit-diff">
          <AuditEntryDiff entry={entry} />
        </div>
      ) : null}
    </div>
  );
}

function AuditEntryDiff({ entry }: { entry: CursorAuditEntry }) {
  const lang = entry.operation.startsWith("structured") ? "json" : "markdown";
  const fileDiff = parseDiffFromFile(
    { name: `${entry.path} (before)`, contents: entry.beforeContent ?? "", lang },
    { name: `${entry.path} (after)`, contents: entry.afterContent ?? "", lang },
  );
  return (
    <FileDiff
      fileDiff={fileDiff}
      disableWorkerPool
      options={{
        diffStyle: "unified",
        overflow: "wrap",
        themeType: "system",
        lineDiffType: "word",
        disableVirtualizationBuffers: true,
      }}
    />
  );
}

class RemoteCursorNameModal extends Modal {
  private plugin: RealtimePlugin;
  private vault: VaultInfo;
  private refresh: () => void;
  private cursor?: RemoteCursorInfo;
  private root: Root | null = null;

  constructor(
    app: App,
    plugin: RealtimePlugin,
    vault: VaultInfo,
    refresh: () => void,
    cursor?: RemoteCursorInfo,
  ) {
    super(app);
    this.plugin = plugin;
    this.vault = vault;
    this.refresh = refresh;
    this.cursor = cursor;
  }

  onOpen(): void {
    this.root = createRoot(this.contentEl);
    this.root.render(
      <RemoteCursorNameView
        plugin={this.plugin}
        vault={this.vault}
        cursor={this.cursor}
        refresh={this.refresh}
        close={() => this.close()}
      />,
    );
  }

  onClose(): void {
    this.root?.unmount();
    this.root = null;
    this.contentEl.empty();
  }
}

function RemoteCursorNameView({
  plugin,
  vault,
  cursor,
  refresh,
  close,
}: {
  plugin: RealtimePlugin;
  vault: VaultInfo;
  cursor?: RemoteCursorInfo;
  refresh: () => void;
  close: () => void;
}) {
  const [name, setName] = useState(cursor?.name ?? "");
  const [secretToken, setSecretToken] = useState("");
  const [createdCursor, setCreatedCursor] = useState<RemoteCursorInfo | null>(null);
  const [busy, setBusy] = useState(false);
  const renameMode = !!cursor;
  return (
    <>
      <h3>{renameMode ? "Rename Remote Cursor" : "Add Remote Cursor"}</h3>
      <p className="setting-item-description">
        {renameMode
          ? "Update the display name for this remote cursor."
          : "Name this remote cursor. MCP servers use the MCP URL for OIDC OAuth. Direct API requests use the secret token as a Bearer token."}
      </p>
      <input
        className="realtime-modal-input"
        type="text"
        value={name}
        onChange={(event) => setName(event.currentTarget.value)}
      />
      {secretToken ? (
        <div className="realtime-warning-box">
          <strong>Copy this secret token now.</strong>
          <p>
            It will not be shown again. Use it as an API Bearer token. Regenerating later
            invalidates this token.
          </p>
          {createdCursor ? (
            <p>
              MCP URL: <code>{createdCursor.mcpUrl}</code>
            </p>
          ) : null}
          <div className="realtime-actions">
            <code>{secretToken}</code>
            <button
              onClick={() => void copyText(secretToken, `${PLUGIN_NAME}: secret token copied.`)}
            >
              Copy
            </button>
          </div>
        </div>
      ) : null}
      <div className="realtime-actions">
        <button onClick={close}>{secretToken ? "Close" : "Cancel"}</button>
        {!secretToken ? (
          <button
            className="mod-cta"
            disabled={busy || !name.trim()}
            onClick={() =>
              void runNotice(setBusy, async () => {
                if (cursor) {
                  await plugin.auth.renameCursor(vault.id, cursor.id, name.trim());
                  new Notice(`${PLUGIN_NAME}: remote cursor renamed.`);
                  refresh();
                  close();
                } else {
                  const created = await plugin.auth.createCursor(vault.id, name.trim());
                  setCreatedCursor(created);
                  setSecretToken(created.secretToken);
                  await copyText(created.secretToken, `${PLUGIN_NAME}: secret token copied.`);
                  refresh();
                }
              })
            }
          >
            {renameMode ? "Rename" : "Create"}
          </button>
        ) : null}
      </div>
    </>
  );
}

function useGitBackup(plugin: RealtimePlugin, vaultId: string) {
  const [reloadKey, setReloadKey] = useState(0);
  const [config, setConfig] = useState<GitBackupConfig | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!vaultId) return;
    let cancelled = false;
    setError("");
    void (async () => {
      try {
        const result = await plugin.auth.getGitBackup(vaultId);
        if (!cancelled) setConfig(result);
      } catch (e) {
        if (!cancelled)
          setError(`Could not load backup settings: ${e instanceof Error ? e.message : String(e)}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [plugin, vaultId, reloadKey]);
  return { config, error, reload: () => setReloadKey((key) => key + 1) };
}

function GitBackupSection({ plugin, vault }: { plugin: RealtimePlugin; vault: VaultInfo }) {
  const { config, error, reload } = useGitBackup(plugin, vault.id);
  const [remoteUrl, setRemoteUrl] = useState("");
  const [branch, setBranch] = useState("main");
  const [authMethod, setAuthMethod] = useState<"ssh" | "https">("ssh");
  const [httpsToken, setHttpsToken] = useState("");
  const [enabled, setEnabled] = useState(true);
  const [busy, setBusy] = useState(false);
  const [testResult, setTestResult] = useState("");

  // Sync the form whenever a (re)load lands.
  useEffect(() => {
    if (!config) return;
    setRemoteUrl(config.remoteUrl ?? "");
    setBranch(config.branch ?? "main");
    setAuthMethod(config.authMethod ?? "ssh");
    setEnabled(config.configured ? config.enabled : true);
    setHttpsToken("");
  }, [config]);

  const save = (regenerateKey = false) =>
    void runNotice(setBusy, async () => {
      await plugin.auth.putGitBackup(vault.id, {
        remoteUrl: remoteUrl.trim(),
        authMethod,
        branch: branch.trim() || "main",
        httpsToken: httpsToken.trim() || undefined,
        regenerateKey,
        enabled,
      });
      new Notice(`${PLUGIN_NAME}: backup settings saved.`);
      setTestResult("");
      reload();
    });

  return (
    <>
      <h3>Git Backup</h3>
      <p className="setting-item-description">
        Push this vault's server-side git history to a remote repository after every commit.
      </p>
      {error ? <p className="realtime-error">{error}</p> : null}
      {!config && !error ? (
        <p className="setting-item-description">Loading backup settings...</p>
      ) : null}
      {config ? (
        <>
          <SettingRow
            name="Remote URL"
            desc={
              authMethod === "ssh"
                ? "SSH remote, e.g. git@github.com:user/repo.git"
                : "HTTPS remote, e.g. https://github.com/user/repo.git"
            }
            control={
              <input
                type="text"
                className="realtime-modal-input"
                value={remoteUrl}
                placeholder={
                  authMethod === "ssh" ? "git@host:user/repo.git" : "https://host/user/repo.git"
                }
                onChange={(event) => setRemoteUrl(event.currentTarget.value)}
              />
            }
          />
          <SettingRow
            name="Branch"
            desc="Remote branch to push to."
            control={
              <input
                type="text"
                className="realtime-modal-input"
                value={branch}
                onChange={(event) => setBranch(event.currentTarget.value)}
              />
            }
          />
          <SettingRow
            name="Authentication"
            desc="A deploy key is generated for you with SSH; use an access token if SSH is blocked."
            control={
              <select
                className="dropdown"
                value={authMethod}
                onChange={(event) => setAuthMethod(event.currentTarget.value as "ssh" | "https")}
              >
                <option value="ssh">SSH deploy key</option>
                <option value="https">HTTPS token</option>
              </select>
            }
          />
          {authMethod === "https" ? (
            <SettingRow
              name="Access token"
              desc={
                config.hasHttpsToken
                  ? "A token is saved. Enter a new one to replace it."
                  : "Personal access token with push permission."
              }
              control={
                <input
                  type="password"
                  className="realtime-modal-input"
                  value={httpsToken}
                  placeholder={config.hasHttpsToken ? "(unchanged)" : ""}
                  onChange={(event) => setHttpsToken(event.currentTarget.value)}
                />
              }
            />
          ) : null}
          <SettingRow
            name="Enable backup"
            desc="Push after every commit when enabled."
            control={
              <input
                type="checkbox"
                checked={enabled}
                onChange={(event) => setEnabled(event.currentTarget.checked)}
              />
            }
          />
          {authMethod === "ssh" && config.sshPublicKey ? (
            <div className="realtime-warning-box">
              <strong>Deploy key</strong>
              <p>
                Add this public key to the remote repository as a deploy key{" "}
                <em>with write access</em>.
              </p>
              <div className="realtime-actions">
                <code className="realtime-deploy-key">{config.sshPublicKey.trim()}</code>
                <button
                  onClick={() =>
                    void copyText(config.sshPublicKey ?? "", `${PLUGIN_NAME}: public key copied.`)
                  }
                >
                  Copy
                </button>
                <button
                  disabled={busy}
                  onClick={() => {
                    if (!confirm("Generate a new deploy key? The current key will stop working."))
                      return;
                    save(true);
                  }}
                >
                  Regenerate
                </button>
              </div>
            </div>
          ) : null}
          {config.configured ? (
            <p className="setting-item-description">
              {config.lastPushError ? (
                <span className="realtime-error">Last push failed: {config.lastPushError}</span>
              ) : config.lastPushAt ? (
                `Last pushed ${new Date(config.lastPushAt).toLocaleString()}.`
              ) : (
                "No push yet — the next vault change will trigger one."
              )}
            </p>
          ) : null}
          {testResult ? <p className="setting-item-description">{testResult}</p> : null}
          <div className="realtime-actions">
            <button className="mod-cta" disabled={busy || !remoteUrl.trim()} onClick={() => save()}>
              {config.configured ? "Save changes" : "Set up backup"}
            </button>
            {config.configured ? (
              <button
                disabled={busy}
                onClick={() =>
                  void runNotice(setBusy, async () => {
                    setTestResult("Testing connection...");
                    const result = await plugin.auth.testGitBackup(vault.id);
                    setTestResult(
                      result.ok
                        ? "Connection OK — the remote is reachable."
                        : `Connection failed: ${result.error ?? "unknown error"}`,
                    );
                  })
                }
              >
                Test connection
              </button>
            ) : null}
            {config.configured ? (
              <button
                className="mod-warning"
                disabled={busy}
                onClick={() =>
                  void runNotice(setBusy, async () => {
                    if (
                      !confirm(
                        "Remove the git backup configuration? The generated deploy key (if any) is deleted.",
                      )
                    )
                      return;
                    await plugin.auth.deleteGitBackup(vault.id);
                    new Notice(`${PLUGIN_NAME}: backup removed.`);
                    setTestResult("");
                    reload();
                  })
                }
              >
                Remove backup
              </button>
            ) : null}
          </div>
        </>
      ) : null}
    </>
  );
}

/**
 * Vault configuration sync controls, modeled on Obsidian Sync's settings
 * panel: a master toggle plus one toggle per configuration category. All
 * toggles are per-device (as in Obsidian Sync) and live in local state so
 * flipping one never re-renders (and scroll-resets) the whole settings page.
 */
function ConfigSyncSection({ plugin }: { plugin: RealtimePlugin }) {
  const [enabled, setEnabled] = useState(plugin.settings.syncConfigEnabled);
  const [categories, setCategories] = useState<ConfigSyncCategories>(() => ({
    ...plugin.settings.configSyncCategories,
  }));
  const configDir = plugin.app.vault.configDir;

  const persist = async () => {
    await plugin.saveSettings();
    await plugin.reloadSync();
  };

  return (
    <>
      <SettingRow
        name="Vault configuration sync"
        desc={`Sync this device's active config folder (${configDir}) between devices using the same folder name, like Obsidian Sync's settings profiles. Configure per device. The Realtime plugin's own folder is always excluded.`}
        control={
          <Toggle
            value={enabled}
            onChange={(value) =>
              void runNotice(undefined, async () => {
                setEnabled(value);
                plugin.settings.syncConfigEnabled = value;
                await persist();
              })
            }
          />
        }
      />
      {enabled
        ? CONFIG_CATEGORIES.map((category) => (
            <SettingRow
              key={category.id}
              name={category.name}
              desc={category.desc}
              control={
                <Toggle
                  value={categories[category.id]}
                  onChange={(value) =>
                    void runNotice(undefined, async () => {
                      setCategories((current) => ({ ...current, [category.id]: value }));
                      plugin.settings.configSyncCategories = {
                        ...plugin.settings.configSyncCategories,
                        [category.id]: value,
                      };
                      await persist();
                    })
                  }
                />
              }
            />
          ))
        : null}
    </>
  );
}

function AdvancedSettings({
  app,
  plugin,
  refresh,
}: {
  app: App;
  plugin: RealtimePlugin;
  refresh: () => void;
}) {
  return (
    <details className="realtime-advanced">
      <summary>Advanced settings</summary>
      <ConfigSyncSection plugin={plugin} />
      <MobileWorkingSetSettings plugin={plugin} />
      <SettingRow
        name="Diagnostic logging"
        desc="Write verbose Realtime diagnostics to the developer console. Keep this off unless troubleshooting."
        control={
          <Toggle
            value={plugin.settings.diagnosticLogging}
            onChange={(value) =>
              void runNotice(undefined, async () => {
                plugin.settings.diagnosticLogging = value;
                await plugin.saveSettings();
                plugin.applyDiagnosticLoggingSetting();
                refresh();
              })
            }
          />
        }
      />
      <p className="setting-item-description">
        Changing the Realtime server URL should usually only be done when resetting or migrating the
        entire vault.
      </p>
      <SettingRow
        name="Realtime server URL"
        desc={plugin.settings.authServerUrl}
        control={
          <button onClick={() => new ServerMigrationModal(app, plugin, refresh).open()}>
            Change server...
          </button>
        }
      />
    </details>
  );
}

export function MobileWorkingSetSettings({ plugin }: { plugin: RealtimePlugin }) {
  const [maxResidentDocs, setMaxResidentDocs] = useState(
    String(plugin.settings.mobileMaxResidentDocs),
  );
  const [recentResidentDocs, setRecentResidentDocs] = useState(
    String(plugin.settings.mobileRecentResidentDocs),
  );

  const saveNumber = async (
    field: "mobileMaxResidentDocs" | "mobileRecentResidentDocs",
    raw: string,
    min: number,
    max: number,
  ) => {
    const parsed = Number(raw);
    const fallback = plugin.settings[field];
    const value = Number.isFinite(parsed)
      ? Math.min(max, Math.max(min, Math.round(parsed)))
      : fallback;
    const previousMax = plugin.settings.mobileMaxResidentDocs;
    const previousRecent = plugin.settings.mobileRecentResidentDocs;
    if (field === "mobileMaxResidentDocs") {
      plugin.settings.mobileMaxResidentDocs = value;
      plugin.settings.mobileRecentResidentDocs = Math.min(
        plugin.settings.mobileRecentResidentDocs,
        value,
      );
    } else {
      plugin.settings.mobileRecentResidentDocs = Math.min(
        value,
        plugin.settings.mobileMaxResidentDocs,
      );
    }
    setMaxResidentDocs(String(plugin.settings.mobileMaxResidentDocs));
    setRecentResidentDocs(String(plugin.settings.mobileRecentResidentDocs));
    if (
      previousMax === plugin.settings.mobileMaxResidentDocs &&
      previousRecent === plugin.settings.mobileRecentResidentDocs
    ) {
      return;
    }
    await plugin.saveSettings();
    await plugin.reloadSync();
  };

  return (
    <>
      <SettingRow
        name="Mobile resident documents"
        desc="Maximum per-file CRDT documents kept in memory on mobile. Open documents are never evicted."
        control={
          <input
            className="realtime-modal-input"
            type="number"
            min={4}
            max={64}
            value={maxResidentDocs}
            onChange={(event) => setMaxResidentDocs(event.currentTarget.value)}
            onBlur={(event) =>
              void saveNumber("mobileMaxResidentDocs", event.currentTarget.value, 4, 64)
            }
          />
        }
      />
      <SettingRow
        name="Mobile recent documents"
        desc="Recently opened documents protected from hibernation within the resident limit."
        control={
          <input
            className="realtime-modal-input"
            type="number"
            min={0}
            max={32}
            value={recentResidentDocs}
            onChange={(event) => setRecentResidentDocs(event.currentTarget.value)}
            onBlur={(event) =>
              void saveNumber("mobileRecentResidentDocs", event.currentTarget.value, 0, 32)
            }
          />
        }
      />
    </>
  );
}

class ServerMigrationModal extends Modal {
  private plugin: RealtimePlugin;
  private refresh: () => void;
  private root: Root | null = null;
  /** Aborted on close: an unfinished migration must never commit afterwards. */
  private readonly closed = new AbortController();

  constructor(app: App, plugin: RealtimePlugin, refresh: () => void) {
    super(app);
    this.plugin = plugin;
    this.refresh = refresh;
  }

  onOpen(): void {
    this.root = createRoot(this.contentEl);
    this.root.render(
      <ServerMigrationView
        app={this.app}
        plugin={this.plugin}
        refresh={this.refresh}
        close={() => this.close()}
        signal={this.closed.signal}
      />,
    );
  }

  onClose(): void {
    this.closed.abort();
    this.root?.unmount();
    this.root = null;
    this.contentEl.empty();
  }
}

function ServerMigrationView({
  app,
  plugin,
  refresh,
  close,
  signal,
}: {
  app: App;
  plugin: RealtimePlugin;
  refresh: () => void;
  close: () => void;
  signal: AbortSignal;
}) {
  const [url, setUrl] = useState(plugin.settings.authServerUrl);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const showPaste = useDelayedFlag(busy, 2000);
  return (
    <>
      <h3>Change Realtime Server</h3>
      <p className="setting-item-description">
        This requires SSO on the new server. The change is only saved if that server has a remote
        vault named "{app.vault.getName()}".
      </p>
      <input
        className="realtime-modal-input"
        type="text"
        value={url}
        onChange={(event) => setUrl(event.currentTarget.value)}
      />
      {error ? <p className="realtime-error">{error}</p> : null}
      <SsoPasteFallback plugin={plugin} visible={showPaste} />
      <div className="realtime-actions">
        <button onClick={close}>Cancel</button>
        <button
          className="mod-cta"
          disabled={busy}
          onClick={() =>
            void (async () => {
              setBusy(true);
              setError("");
              try {
                await plugin.migrateToServer(url, signal);
                new Notice(`${PLUGIN_NAME}: server updated.`);
                close();
                refresh();
              } catch (e) {
                if (!signal.aborted) setError(e instanceof Error ? e.message : String(e));
              } finally {
                if (!signal.aborted) setBusy(false);
              }
            })()
          }
        >
          {busy ? "Waiting for SSO..." : "Test with SSO"}
        </button>
      </div>
    </>
  );
}

function useVaults(plugin: RealtimePlugin) {
  const [reloadKey, setReloadKey] = useState(0);
  const [vaults, setVaults] = useState<VaultInfo[] | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    let cancelled = false;
    setVaults(null);
    setError("");
    void (async () => {
      try {
        const listed = await plugin.auth.listVaults();
        if (!cancelled) setVaults(listed);
      } catch (e) {
        if (!cancelled)
          setError(`Could not load vaults: ${e instanceof Error ? e.message : String(e)}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [plugin, reloadKey]);
  return { vaults, error, reload: () => setReloadKey((key) => key + 1) };
}

function useMembers(plugin: RealtimePlugin, vaultId: string) {
  const [reloadKey, setReloadKey] = useState(0);
  const [members, setMembers] = useState<MemberInfo[] | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!vaultId) return;
    let cancelled = false;
    setMembers(null);
    setError("");
    void (async () => {
      try {
        const listed = await plugin.auth.listMembers(vaultId);
        if (!cancelled) setMembers(listed);
      } catch (e) {
        if (!cancelled)
          setError(`Could not load members: ${e instanceof Error ? e.message : String(e)}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [plugin, vaultId, reloadKey]);
  return { members, error, reload: () => setReloadKey((key) => key + 1) };
}

function useRemoteCursors(plugin: RealtimePlugin, vaultId: string) {
  const [reloadKey, setReloadKey] = useState(0);
  const [cursors, setCursors] = useState<RemoteCursorInfo[] | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!vaultId) return;
    let cancelled = false;
    setCursors(null);
    setError("");
    void (async () => {
      try {
        const listed = await plugin.auth.listCursors(vaultId);
        if (!cancelled) setCursors(listed);
      } catch (e) {
        if (!cancelled)
          setError(`Could not load remote cursors: ${e instanceof Error ? e.message : String(e)}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [plugin, vaultId, reloadKey]);
  return { cursors, error, reload: () => setReloadKey((key) => key + 1) };
}

/** Becomes true `delayMs` after `active` turns true; resets when it turns false. */
function useDelayedFlag(active: boolean, delayMs: number): boolean {
  const [on, setOn] = useState(false);
  useEffect(() => {
    if (!active) {
      setOn(false);
      return;
    }
    const id = window.setTimeout(() => setOn(true), delayMs);
    return () => window.clearTimeout(id);
  }, [active, delayMs]);
  return on;
}

/**
 * Backup for SSO when the `obsidian://` deep link doesn't return to Obsidian:
 * the login page also prints the session code, which the user can paste here to
 * complete the in-flight login.
 */
function SsoPasteFallback({ plugin, visible }: { plugin: RealtimePlugin; visible: boolean }) {
  const [code, setCode] = useState("");
  if (!visible) return null;
  return (
    <div className="realtime-paste-fallback">
      <p className="setting-item-description">
        Not redirected back to Obsidian? Paste the sign-in code shown in your browser:
      </p>
      <div className="realtime-actions">
        <input
          className="realtime-modal-input"
          type="text"
          placeholder="Paste sign-in code"
          value={code}
          onChange={(event) => setCode(event.currentTarget.value.trim())}
        />
        <button
          type="button"
          className="mod-cta"
          disabled={!code}
          onClick={() => {
            if (!plugin.auth.submitPastedCode(code)) {
              new Notice(`${PLUGIN_NAME}: no sign-in is waiting for a code. Start sign-in again.`);
            }
          }}
        >
          Use code
        </button>
      </div>
    </div>
  );
}

async function runNotice(
  setBusy: ((busy: boolean) => void) | undefined,
  fn: () => Promise<void>,
): Promise<void> {
  try {
    setBusy?.(true);
    await fn();
  } catch (e) {
    new Notice(`${PLUGIN_NAME}: ${e instanceof Error ? e.message : String(e)}`);
  } finally {
    setBusy?.(false);
  }
}

async function copyText(text: string, message: string): Promise<void> {
  await navigator.clipboard?.writeText(text);
  new Notice(message);
}

function SettingRow({
  name,
  desc,
  control,
}: {
  name: ReactNode;
  desc?: ReactNode;
  control: ReactNode;
}) {
  return (
    <div className="setting-item">
      <div className="setting-item-info">
        <div className="setting-item-name">{name}</div>
        {desc ? <div className="setting-item-description">{desc}</div> : null}
      </div>
      <div className="setting-item-control">{control}</div>
    </div>
  );
}
