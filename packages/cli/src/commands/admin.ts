import type { Command } from "commander";
import { CliError } from "../config";
import { ctxFrom, out, userVault, vaultClients, workspace } from "../context";
import { requireUserClient } from "../client";
import { formatBytes, formatTime, printTable } from "../output";

export function registerAdminCommands(program: Command): void {
  const vaultCmd = program.command("vault").description("manage vaults (user login required)");

  vaultCmd
    .command("list")
    .description("list your vaults")
    .action(async () => {
      const ctx = ctxFrom(program);
      const ws = workspace(ctx);
      const client = requireUserClient(ws);
      const vaults = await client.vaults.list();
      out(ctx, vaults, () =>
        printTable(
          vaults.map((v) => [
            v.id,
            v.name,
            v.role + (v.owner ? " (owner)" : ""),
            v.id === ws.config.vaultId ? "*" : "",
          ]),
          ["id", "name", "role", ""],
        ),
      );
    });

  vaultCmd
    .command("create <name>")
    .description("create a new empty vault (does not rebind this folder)")
    .action(async (name: string) => {
      const ctx = ctxFrom(program);
      const client = requireUserClient(workspace(ctx));
      const vault = await client.vaults.create(name);
      out(ctx, vault, () => process.stdout.write(`created vault "${vault.name}" (${vault.id})\n`));
    });

  const members = program.command("members").description("vault membership (user login required)");

  members
    .command("list")
    .description("list vault members")
    .action(async () => {
      const ctx = ctxFrom(program);
      const { vault } = userVault(ctx);
      const list = await vault.members.list();
      out(ctx, list, () =>
        printTable(
          list.map((m) => [m.userId, m.displayName, m.email, m.role + (m.owner ? " (owner)" : "")]),
          ["id", "name", "email", "role"],
        ),
      );
    });

  members
    .command("promote <userId>")
    .description("promote a member to admin")
    .action(async (userId: string) => {
      const ctx = ctxFrom(program);
      const { vault } = userVault(ctx);
      const m = await vault.members.promote(userId);
      out(ctx, m, () => process.stdout.write(`${m.displayName} is now ${m.role}\n`));
    });

  members
    .command("rm <userId>")
    .description("remove a member from the vault")
    .action(async (userId: string) => {
      const ctx = ctxFrom(program);
      const { vault } = userVault(ctx);
      await vault.members.remove(userId);
      process.stderr.write(`removed ${userId}\n`);
    });

  const invite = program.command("invite").description("vault invites (user login required)");

  invite
    .command("create")
    .description("mint an invite code for this vault")
    .option("--role <role>", "admin or member", "member")
    .action(async (opts: { role: string }) => {
      const ctx = ctxFrom(program);
      const ws = workspace(ctx);
      const client = requireUserClient(ws);
      if (opts.role !== "admin" && opts.role !== "member")
        throw new CliError("--role must be admin or member");
      const res = await client.invites.create(ws.config.vaultId, { role: opts.role });
      out(ctx, res, () => process.stdout.write(`invite code: ${res.code}\n`));
    });

  invite
    .command("redeem <code>")
    .description("redeem an invite code")
    .action(async (code: string) => {
      const ctx = ctxFrom(program);
      const client = requireUserClient(workspace(ctx));
      const res = await client.invites.redeem(code);
      out(ctx, res, () => process.stdout.write(`joined vault "${res.name}" (${res.vaultId})\n`));
    });

  program
    .command("storage")
    .description("show vault storage usage")
    .option("--gc", "garbage-collect orphaned blobs")
    .option("--min-bytes <n>", "with --gc: only blobs at least this large", (v) => parseInt(v, 10))
    .option(
      "--min-age <seconds>",
      "with --gc: only blobs written at least this long ago (server default: 3600)",
      (v) => parseInt(v, 10),
    )
    .action(async (opts: { gc?: boolean; minBytes?: number; minAge?: number }) => {
      const ctx = ctxFrom(program);
      const { vault } = vaultClients(ctx);
      if (opts.gc) {
        const res = await vault.storage.gcBlobs({
          minBytes: opts.minBytes,
          minAgeSeconds: opts.minAge,
        });
        out(ctx, res, () =>
          process.stdout.write(
            `removed ${res.removed} blob(s), freed ${formatBytes(res.freedBytes)}\n`,
          ),
        );
        return;
      }
      const usage = await vault.storage.usage();
      out(ctx, usage, () => {
        process.stdout.write(
          `blobs (current):  ${usage.currentBlobCount} (${formatBytes(usage.blobsCurrentBytes)})\n`,
        );
        process.stdout.write(
          `blobs (previous): ${usage.previousBlobCount} (${formatBytes(usage.blobsPreviousBytes)})\n`,
        );
        if (usage.plainVaultBytes !== null) {
          process.stdout.write(`vault text:       ${formatBytes(usage.plainVaultBytes)}\n`);
        }
      });
    });

  const backup = program
    .command("backup")
    .description("git backup configuration (user login required)");

  backup
    .command("get")
    .description("show the backup configuration")
    .action(async () => {
      const ctx = ctxFrom(program);
      const { vault } = userVault(ctx);
      const cfg = await vault.backup.get();
      out(ctx, cfg, () => {
        if (!cfg.configured) {
          process.stdout.write("no backup configured\n");
          return;
        }
        process.stdout.write(
          `remote:  ${cfg.remoteUrl} (${cfg.authMethod}, branch ${cfg.branch ?? "default"})\n`,
        );
        process.stdout.write(`enabled: ${cfg.enabled}\n`);
        if (cfg.sshPublicKey) process.stdout.write(`ssh key: ${cfg.sshPublicKey}\n`);
        if (cfg.lastPushAt) process.stdout.write(`last push: ${formatTime(cfg.lastPushAt)}\n`);
        if (cfg.lastPushError) process.stdout.write(`last error: ${cfg.lastPushError}\n`);
      });
    });

  backup
    .command("set <remoteUrl>")
    .description("configure the backup remote")
    .option("--ssh", "authenticate over ssh (default)")
    .option("--https-token <token>", "authenticate over https with a token")
    .option("--branch <branch>", "target branch")
    .option("--disabled", "configure but leave disabled")
    .option("--regenerate-key", "mint a fresh ssh keypair")
    .action(
      async (
        remoteUrl: string,
        opts: {
          ssh?: boolean;
          httpsToken?: string;
          branch?: string;
          disabled?: boolean;
          regenerateKey?: boolean;
        },
      ) => {
        const ctx = ctxFrom(program);
        const { vault } = userVault(ctx);
        const cfg = await vault.backup.put({
          remoteUrl,
          authMethod: opts.httpsToken ? "https" : "ssh",
          httpsToken: opts.httpsToken,
          branch: opts.branch,
          regenerateKey: opts.regenerateKey,
          enabled: !opts.disabled,
        });
        out(ctx, cfg, () => {
          process.stdout.write(`backup configured: ${cfg.remoteUrl}\n`);
          if (cfg.sshPublicKey)
            process.stdout.write(`add this deploy key to the remote:\n${cfg.sshPublicKey}\n`);
        });
      },
    );

  backup
    .command("rm")
    .description("remove the backup configuration")
    .action(async () => {
      const ctx = ctxFrom(program);
      const { vault } = userVault(ctx);
      await vault.backup.delete();
      process.stderr.write("backup configuration removed\n");
    });

  backup
    .command("test")
    .description("test the configured remote without pushing")
    .action(async () => {
      const ctx = ctxFrom(program);
      const { vault } = userVault(ctx);
      const res = await vault.backup.test();
      out(ctx, res, () => process.stdout.write(`${JSON.stringify(res)}\n`));
    });
}
