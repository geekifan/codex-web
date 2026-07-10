#!/usr/bin/env node

import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { parseArgs as parseCliArgs } from "node:util";
import { WebSocket, WebSocketServer } from "ws";
import Fastify from "fastify";
import fastifyMultipart from "@fastify/multipart";
import fastifyStatic from "@fastify/static";
import { installModuleAliasHook } from "./module";
import {
  parseReliableBridgeHello,
  ReliableBridgeSession,
} from "./reliable-bridge";
import { glob } from "glob";
import { invokeIpcMainHandlerForServer } from "./electron";
import {
  createProjectWritableRootsService,
  type ProjectWritableRoots,
  type ProjectWritableRootsResult,
} from "./project-writable-roots";
import {
  createThreadProjectAssignmentsService,
  threadProjectAssignmentsValue,
  type ThreadProjectAssignment,
  type ThreadProjectAssignments,
} from "./thread-project-assignments";

type ServerOptions = {
  host: string;
  port: number;
};

type RendererToMainMessage =
  | {
      type: "ipc-renderer-invoke";
      requestId: string;
      channel: string;
      args: unknown[];
      sourceUrl: string;
    }
  | {
      type: "ipc-renderer-send";
      channel: string;
      args: unknown[];
      sourceUrl: string;
    }
  | {
      type: "workspace-directory-entries-request";
      requestId: string;
      directoryPath: string | null;
      directoriesOnly: boolean;
    }
  | {
      type: "project-writable-roots-request";
      requestId: string;
      operation: "addRoot" | "clearRoots" | "removeRoot";
      legacyRoot: string | null;
      projectId: string;
      root?: string;
    }
  | {
      type: "thread-project-assignment-request";
      requestId: string;
      threadId: string;
      assignment: ThreadProjectAssignment | null;
    };

type MainToRendererMessage =
  | {
      type: "ipc-main-event";
      channel: string;
      args: unknown[];
    }
  | {
      type: "ipc-renderer-invoke-result";
      requestId: string;
      ok: true;
      result: unknown;
    }
  | {
      type: "ipc-renderer-invoke-result";
      requestId: string;
      ok: false;
      errorMessage: string;
    }
  | {
      type: "workspace-directory-entries-result";
      requestId: string;
      ok: true;
      result: WorkspaceDirectoryEntries;
    }
  | {
      type: "workspace-directory-entries-result";
      requestId: string;
      ok: false;
      errorMessage: string;
    }
  | {
      type: "project-writable-roots-result";
      requestId: string;
      ok: true;
      result: ProjectWritableRootsResult;
    }
  | {
      type: "project-writable-roots-result";
      requestId: string;
      ok: false;
      errorMessage: string;
    }
  | {
      type: "thread-project-assignment-result";
      requestId: string;
      ok: true;
    }
  | {
      type: "thread-project-assignment-result";
      requestId: string;
      ok: false;
      errorMessage: string;
    };

type WorkspaceDirectoryEntry = {
  name: string;
  path: string;
  type: "directory" | "file";
};

type WorkspaceDirectoryEntries = {
  directoryPath: string;
  parentPath: string | null;
  entries: WorkspaceDirectoryEntry[];
};

function workspaceDirectoryEntryTypeRank(
  entry: WorkspaceDirectoryEntry,
): number {
  return entry.type === "directory" ? 0 : 1;
}

function workspaceDirectoryEntryHiddenRank(
  entry: WorkspaceDirectoryEntry,
): number {
  return entry.name.startsWith(".") ? 1 : 0;
}

function compareWorkspaceDirectoryEntries(
  left: WorkspaceDirectoryEntry,
  right: WorkspaceDirectoryEntry,
): number {
  return (
    workspaceDirectoryEntryTypeRank(left) -
      workspaceDirectoryEntryTypeRank(right) ||
    workspaceDirectoryEntryHiddenRank(left) -
      workspaceDirectoryEntryHiddenRank(right) ||
    left.name.localeCompare(right.name)
  );
}

type IpcMainBridgeState = {
  broadcastToRenderer?: (message: MainToRendererMessage) => void;
  handleRendererInvoke?: (channel: string, args: unknown[]) => Promise<unknown>;
  handleRendererSend?: (channel: string, args: unknown[]) => void;
};

function printUsage(): void {
  console.log(
    [
      "Usage:",
      "  server [--host <host>] [--port <port>]",
      "",
      "Defaults:",
      "  --host 127.0.0.1",
      "  --port 8214",
      "",
      "Examples:",
      "  yarn server",
      "  yarn server --port 9000",
    ].join("\n"),
  );
}

function parsePort(raw: string): number {
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65535) {
    throw new Error(`Invalid port: ${raw}`);
  }
  return parsed;
}

function parseServerArgs(args: string[]): ServerOptions {
  const parsed = parseCliArgs({
    args,
    allowPositionals: false,
    options: {
      help: {
        short: "h",
        type: "boolean",
      },
      host: {
        type: "string",
      },
      port: {
        type: "string",
      },
    },
    strict: true,
  });

  if (parsed.values.help) {
    printUsage();
    process.exit(0);
  }

  return {
    host: parsed.values.host ?? "127.0.0.1",
    port: parsed.values.port ? parsePort(parsed.values.port) : 8214,
  };
}

function getIpcMainBridgeState(): IpcMainBridgeState {
  const globals = globalThis as typeof globalThis & {
    __codexElectronIpcBridge?: IpcMainBridgeState;
  };
  if (!globals.__codexElectronIpcBridge) {
    globals.__codexElectronIpcBridge = {};
  }
  return globals.__codexElectronIpcBridge;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.stack ?? error.message;
  }
  return String(error);
}

const CODEX_MESSAGE_FROM_VIEW = "codex_desktop:message-from-view";
const CODEX_MESSAGE_FOR_VIEW = "codex_desktop:message-for-view";
const PROJECT_WRITABLE_ROOTS_KEY = "project-writable-roots";
const THREAD_PROJECT_ASSIGNMENTS_KEY = "thread-project-assignments";

type CodexFetchResponse = {
  type: "fetch-response";
  requestId: string;
  responseType: "error" | "success";
  status: number;
  bodyJsonString?: string;
  error?: string;
};

function isCodexFetchResponse(value: unknown): value is CodexFetchResponse {
  return (
    typeof value === "object" &&
    value !== null &&
    "type" in value &&
    value.type === "fetch-response" &&
    "requestId" in value &&
    typeof value.requestId === "string" &&
    "responseType" in value &&
    (value.responseType === "success" || value.responseType === "error") &&
    "status" in value &&
    typeof value.status === "number"
  );
}

function projectWritableRootsValue(value: unknown): ProjectWritableRoots {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return {};
  }
  const roots: ProjectWritableRoots = {};
  for (const [projectId, entries] of Object.entries(value)) {
    if (!Array.isArray(entries)) {
      continue;
    }
    const validEntries = entries.flatMap((entry) =>
      typeof entry === "object" &&
      entry !== null &&
      "kind" in entry &&
      entry.kind === "local" &&
      "path" in entry &&
      typeof entry.path === "string"
        ? [{ kind: "local" as const, path: entry.path }]
        : [],
    );
    roots[projectId] = validEntries;
  }
  return roots;
}

async function postCodexRequest<T>(
  pathName: string,
  params: unknown,
): Promise<T> {
  const requestId = randomUUID();
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Timed out waiting for Codex request: ${pathName}`));
    }, 30_000);
    timeout.unref?.();

    function finish(callback: () => void): void {
      clearTimeout(timeout);
      callback();
    }

    void invokeIpcMainHandlerForServer(
      CODEX_MESSAGE_FROM_VIEW,
      [
        {
          body: JSON.stringify(params),
          method: "POST",
          requestId,
          type: "fetch",
          url: `vscode://codex/${pathName}`,
        },
      ],
      (channel, args) => {
        if (channel !== CODEX_MESSAGE_FOR_VIEW) {
          return;
        }
        const response = args[0];
        if (
          !isCodexFetchResponse(response) ||
          response.requestId !== requestId
        ) {
          return;
        }
        if (
          response.responseType !== "success" ||
          response.status < 200 ||
          response.status >= 300
        ) {
          finish(() =>
            reject(
              new Error(
                response.error ??
                  `Codex request failed with status ${response.status}`,
              ),
            ),
          );
          return;
        }
        try {
          finish(() =>
            resolve(JSON.parse(response.bodyJsonString ?? "null") as T),
          );
        } catch (error) {
          finish(() => reject(error));
        }
      },
    ).catch((error) => finish(() => reject(error)));
  });
}

async function getProjectWritableRoots(): Promise<ProjectWritableRoots> {
  const response = await postCodexRequest<{ value?: unknown }>(
    "get-global-state",
    { key: PROJECT_WRITABLE_ROOTS_KEY },
  );
  return projectWritableRootsValue(response.value);
}

async function setProjectWritableRoots(
  value: ProjectWritableRoots,
): Promise<void> {
  const response = await postCodexRequest<{ success?: unknown }>(
    "set-global-state",
    { key: PROJECT_WRITABLE_ROOTS_KEY, value },
  );
  if (response.success !== true) {
    throw new Error("Failed to set project writable roots");
  }
}

async function getThreadProjectAssignments(): Promise<ThreadProjectAssignments> {
  const response = await postCodexRequest<{ value?: unknown }>(
    "get-global-state",
    { key: THREAD_PROJECT_ASSIGNMENTS_KEY },
  );
  return threadProjectAssignmentsValue(response.value);
}

async function setThreadProjectAssignments(
  value: ThreadProjectAssignments,
): Promise<void> {
  const response = await postCodexRequest<{ success?: unknown }>(
    "set-global-state",
    { key: THREAD_PROJECT_ASSIGNMENTS_KEY, value },
  );
  if (response.success !== true) {
    throw new Error("Failed to set thread project assignments");
  }
}

async function getWorkspaceDirectoryEntries({
  directoryPath,
  directoriesOnly,
}: {
  directoryPath: string | null;
  directoriesOnly: boolean;
}): Promise<WorkspaceDirectoryEntries> {
  const requestedPath = directoryPath?.trim() || os.homedir();
  const resolvedPath = path.resolve(requestedPath);
  const stat = await fs.stat(resolvedPath);
  if (!stat.isDirectory()) {
    throw new Error(`Directory not found: ${requestedPath}`);
  }

  const entries = (await fs.readdir(resolvedPath, { withFileTypes: true }))
    .flatMap((entry): WorkspaceDirectoryEntry[] => {
      const type = entry.isDirectory() ? "directory" : "file";
      if (directoriesOnly && type !== "directory") {
        return [];
      }

      return [
        {
          name: entry.name,
          path: path.join(resolvedPath, entry.name),
          type,
        },
      ];
    })
    .sort(compareWorkspaceDirectoryEntries);

  const rootPath = path.parse(resolvedPath).root;
  const parentPath =
    resolvedPath === rootPath ? null : path.dirname(resolvedPath);

  return {
    directoryPath: resolvedPath,
    parentPath,
    entries,
  };
}

function ensureElectronLikeProcessContext(): void {
  const versions = process.versions as NodeJS.ProcessVersions & {
    electron?: string;
  };
  if (!versions.electron) {
    Object.defineProperty(versions, "electron", {
      value: "41.2.0",
      configurable: true,
      enumerable: true,
      writable: false,
    });
  }

  const processWithElectronFields = process as NodeJS.Process & {
    resourcesPath?: string;
    type?: string;
  };
  processWithElectronFields.resourcesPath ??= path.resolve(
    __dirname,
    "../../scratch/asar",
  );
  processWithElectronFields.type ??= "browser";
}

function sendBridgeReset(socket: WebSocket, reason: string): void {
  if (socket.readyState === WebSocket.OPEN) {
    try {
      socket.send(JSON.stringify({ type: "bridge-reset", reason }));
    } catch {
      socket.terminate();
      return;
    }
  }
  socket.close(1008, reason);
}

async function startIpcBridgeServer(options: ServerOptions): Promise<void> {
  const bridgeState = getIpcMainBridgeState();
  const app = Fastify({ logger: false });
  const websocketServer = new WebSocketServer({ noServer: true });
  const serverEpoch = randomUUID();
  const sessions = new Map<
    string,
    ReliableBridgeSession<RendererToMainMessage, MainToRendererMessage>
  >();
  const projectWritableRoots = createProjectWritableRootsService({
    getValue: getProjectWritableRoots,
    notifyChanged: () => {
      bridgeState.broadcastToRenderer?.({
        type: "ipc-main-event",
        channel: CODEX_MESSAGE_FOR_VIEW,
        args: [
          {
            type: "global-state-updated",
            keys: [PROJECT_WRITABLE_ROOTS_KEY],
          },
        ],
      });
    },
    setValue: setProjectWritableRoots,
  });
  const threadProjectAssignments = createThreadProjectAssignmentsService({
    getValue: getThreadProjectAssignments,
    notifyChanged: ({ threadId, assignment }) => {
      bridgeState.broadcastToRenderer?.({
        type: "ipc-main-event",
        channel: CODEX_MESSAGE_FOR_VIEW,
        args: [
          {
            type: "thread-project-assignment-updated",
            threadId,
            assignment,
          },
        ],
      });
    },
    setValue: setThreadProjectAssignments,
  });

  await app.register(fastifyMultipart, {
    limits: {
      fileSize: Infinity,
    },
  });

  const uploadRoot = await fs.mkdtemp(
    path.join(os.tmpdir(), "codex-web-uploads-"),
  );

  app.post("/__backend/upload", async (request, reply) => {
    if (!request.isMultipart()) {
      return reply.code(400).send({ error: "expected multipart upload body" });
    }

    const files = await Array.fromAsync(
      (async function* () {
        for await (const part of request.files()) {
          const label = part.filename?.trim() || "upload";

          const uploadedPath = path.join(uploadRoot, randomUUID());

          await fs.writeFile(uploadedPath, await part.toBuffer());

          yield {
            label,
            path: uploadedPath,
            fsPath: uploadedPath,
          };
        }
      })(),
    );

    return reply.send({ files });
  });

  await app.register(fastifyStatic, {
    root: "/",
    prefix: "/@fs/",
    decorateReply: false,
  });

  await app.register(fastifyStatic, {
    root: path.resolve(__dirname, "../../scratch/asar/webview"),
    prefix: "/",
  });

  app.get("/", async (_request, reply) => {
    return reply.sendFile("index.html");
  });

  app.setNotFoundHandler((request, reply) => {
    if (request.url.startsWith("/@fs/")) {
      return reply.code(404).send({ error: "Not Found" });
    }

    if (request.method === "GET") {
      return reply.sendFile("index.html");
    }
    return reply.code(404).send({ error: "Not Found" });
  });

  app.server.on("upgrade", (request, socket, head) => {
    const requestUrl = request.url ?? "/";
    const host = request.headers.host ?? "localhost";
    const url = new URL(requestUrl, `http://${host}`);
    if (url.pathname !== "/__backend/ipc") {
      socket.destroy();
      return;
    }

    websocketServer.handleUpgrade(request, socket, head, (upgradedSocket) => {
      websocketServer.emit("connection", upgradedSocket, request);
    });
  });

  bridgeState.broadcastToRenderer = (message: MainToRendererMessage): void => {
    for (const session of sessions.values()) {
      session.send(message);
    }
  };

  websocketServer.on("connection", (socket) => {
    const handshakeTimeout = setTimeout(() => {
      socket.close(1008, "reliable bridge handshake timed out");
    }, 10_000);
    handshakeTimeout.unref?.();
    socket.once("close", () => {
      clearTimeout(handshakeTimeout);
    });

    socket.once("message", (rawData) => {
      clearTimeout(handshakeTimeout);
      let value: unknown;
      try {
        value = JSON.parse(String(rawData));
      } catch {
        sendBridgeReset(socket, "invalid reliable bridge handshake");
        return;
      }

      const hello = parseReliableBridgeHello(value);
      if (!hello) {
        sendBridgeReset(socket, "invalid reliable bridge handshake");
        return;
      }

      if (hello.serverEpoch !== null && hello.serverEpoch !== serverEpoch) {
        sendBridgeReset(socket, "backend restarted");
        return;
      }

      let session = sessions.get(hello.connectionId);
      if (!session) {
        if (hello.serverEpoch !== null) {
          sendBridgeReset(socket, "reconnection session expired");
          return;
        }

        session = new ReliableBridgeSession({
          connectionId: hello.connectionId,
          serverEpoch,
          onDispose: () => {
            sessions.delete(hello.connectionId);
          },
          onMessage: (message) => {
            handleRendererMessage(session!, message);
          },
        });
        sessions.set(hello.connectionId, session);
      }

      session.attach(socket);
    });
  });

  function handleRendererMessage(
    session: ReliableBridgeSession<
      RendererToMainMessage,
      MainToRendererMessage
    >,
    message: RendererToMainMessage,
  ): void {
    if (message.type === "ipc-renderer-send") {
      bridgeState.handleRendererSend?.(message.channel, message.args);
      return;
    }

    if (message.type === "workspace-directory-entries-request") {
      const { requestId } = message;
      getWorkspaceDirectoryEntries(message)
        .then((result) => {
          session.send({
            type: "workspace-directory-entries-result",
            requestId,
            ok: true,
            result,
          });
        })
        .catch((error) => {
          session.send({
            type: "workspace-directory-entries-result",
            requestId,
            ok: false,
            errorMessage: errorMessage(error),
          });
        });
      return;
    }

    if (message.type === "project-writable-roots-request") {
      const { requestId } = message;
      const request = {
        legacyRoot: message.legacyRoot,
        projectId: message.projectId,
        root: message.root,
      };
      const operation =
        message.operation === "addRoot"
          ? projectWritableRoots.addRoot(request)
          : message.operation === "clearRoots"
            ? projectWritableRoots.clearRoots(request)
            : message.root === undefined
              ? Promise.reject(new Error("Project writable root is required"))
              : projectWritableRoots.removeRoot({
                  ...request,
                  root: message.root,
                });
      operation
        .then((result) => {
          session.send({
            type: "project-writable-roots-result",
            requestId,
            ok: true,
            result,
          });
        })
        .catch((error) => {
          session.send({
            type: "project-writable-roots-result",
            requestId,
            ok: false,
            errorMessage: errorMessage(error),
          });
        });
      return;
    }

    if (message.type === "thread-project-assignment-request") {
      const { requestId, threadId, assignment } = message;
      threadProjectAssignments
        .setAssignment({ threadId, assignment })
        .then(() => {
          session.send({
            type: "thread-project-assignment-result",
            requestId,
            ok: true,
          });
        })
        .catch((error) => {
          session.send({
            type: "thread-project-assignment-result",
            requestId,
            ok: false,
            errorMessage: errorMessage(error),
          });
        });
      return;
    }

    if (message.type === "ipc-renderer-invoke") {
      const { channel, requestId, args } = message;
      Promise.resolve(
        bridgeState.handleRendererInvoke?.(channel, args) ??
          Promise.reject(
            new Error(`[ipc-bridge] no ipcMain.handle for channel ${channel}`),
          ),
      )
        .then((result) => {
          session.send({
            type: "ipc-renderer-invoke-result",
            requestId,
            ok: true,
            result,
          });
        })
        .catch((error) => {
          session.send({
            type: "ipc-renderer-invoke-result",
            requestId,
            ok: false,
            errorMessage: errorMessage(error),
          });
        });
    }
  }

  await app.listen({ host: options.host, port: options.port });
  console.log(`IPC bridge listening at ws://${options.host}:${options.port}`);

  ensureElectronLikeProcessContext();
  installModuleAliasHook();

  const matches = await glob("../../scratch/asar/.vite/build/main-*.js", {
    nodir: true,
    cwd: __dirname,
  });

  if (matches.length === 0) {
    throw new Error("no main bundle found");
  }

  if (matches.length > 1) {
    throw new Error("multiple main bundles found");
  }

  const module = require(matches[0]!);
  module.runMainAppStartup();
}

async function main(args: string[]) {
  const options = parseServerArgs(args);

  await startIpcBridgeServer(options);
}

main(process.argv.slice(2));
