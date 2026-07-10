import { createHash, randomUUID } from "node:crypto";
import fs, { type FileHandle } from "node:fs/promises";
import path from "node:path";
import {
  WORKSPACE_FILE_CHUNK_BYTES,
  WORKSPACE_FILE_MAX_BYTES,
  type WorkspaceFileMainMessage,
  type WorkspaceFileRendererMessage,
} from "../shared/workspace-files";

type SendWorkspaceFileMessage = (
  message: WorkspaceFileMainMessage,
  waitForAcknowledgement?: boolean,
) => Promise<void> | void;

type WriteTransfer = {
  byteLength: number;
  expectedEtag: string;
  filePath: string;
  handle: FileHandle;
  position: number;
  requestId: string;
  temporaryPath: string;
  transferId: string;
};

const pathLocks = new Map<string, Promise<void>>();

export class WorkspaceFileManager {
  private readonly reads = new Map<string, AbortController>();
  private readonly writes = new Map<string, WriteTransfer>();
  private disposed = false;

  constructor(private readonly send: SendWorkspaceFileMessage) {}

  handle(message: WorkspaceFileRendererMessage): void {
    if (this.disposed) {
      return;
    }
    switch (message.type) {
      case "workspace-file-read-request":
        void this.read(message);
        return;
      case "workspace-file-write-start":
        void this.startWrite(message);
        return;
      case "workspace-file-write-chunk":
        void this.writeChunk(message);
        return;
      case "workspace-file-write-commit":
        void this.commitWrite(message);
        return;
      case "workspace-file-write-abort":
        void this.abortWrite(message.transferId);
    }
  }

  async dispose(): Promise<void> {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    for (const controller of this.reads.values()) {
      controller.abort();
    }
    this.reads.clear();
    await Promise.all(
      Array.from(this.writes.keys(), (transferId) =>
        this.abortWrite(transferId),
      ),
    );
  }

  private async read(
    message: Extract<
      WorkspaceFileRendererMessage,
      { type: "workspace-file-read-request" }
    >,
  ): Promise<void> {
    const transferId = randomUUID();
    const controller = new AbortController();
    this.reads.set(transferId, controller);
    let handle: FileHandle | undefined;
    try {
      assertLocalHost(message.hostId);
      const filePath = resolveFilePath(message.path);
      handle = await fs.open(filePath, "r");
      const initialStat = await handle.stat();
      if (!initialStat.isFile()) {
        throw new Error("workspace file path is not a file");
      }
      if (initialStat.size > WORKSPACE_FILE_MAX_BYTES) {
        throw new Error(
          `workspace file exceeds ${WORKSPACE_FILE_MAX_BYTES} bytes`,
        );
      }
      const representation = await resolveRepresentation(
        handle,
        initialStat.size,
        message.representation,
      );
      const etag = workspaceFileEtag(initialStat);
      await this.sendAndWait({
        type: "workspace-file-read-start",
        requestId: message.requestId,
        transferId,
        etag,
        byteLength: initialStat.size,
        representation,
      });

      let offset = 0;
      while (offset < initialStat.size) {
        throwIfAborted(controller.signal);
        const buffer = new Uint8Array(
          Math.min(WORKSPACE_FILE_CHUNK_BYTES, initialStat.size - offset),
        );
        const { bytesRead } = await handle.read(
          buffer,
          0,
          buffer.byteLength,
          offset,
        );
        if (bytesRead === 0) {
          break;
        }
        await this.sendAndWait({
          type: "workspace-file-read-chunk",
          requestId: message.requestId,
          transferId,
          offset,
          bytes: buffer.subarray(0, bytesRead),
        });
        offset += bytesRead;
      }

      const finalStat = await handle.stat();
      if (
        offset !== initialStat.size ||
        workspaceFileEtag(finalStat) !== etag
      ) {
        throw new Error("workspace file changed while it was being read");
      }
      await this.sendAndWait({
        type: "workspace-file-read-end",
        requestId: message.requestId,
        transferId,
      });
    } catch (error) {
      if (!this.disposed && !controller.signal.aborted) {
        await Promise.resolve(
          this.send({
            type: "workspace-file-read-error",
            requestId: message.requestId,
            transferId,
            errorMessage: errorMessage(error),
          }),
        ).catch(() => {});
      }
    } finally {
      this.reads.delete(transferId);
      await handle?.close().catch(() => {});
    }
  }

  private async startWrite(
    message: Extract<
      WorkspaceFileRendererMessage,
      { type: "workspace-file-write-start" }
    >,
  ): Promise<void> {
    try {
      assertLocalHost(message.hostId);
      if (
        !Number.isSafeInteger(message.byteLength) ||
        message.byteLength < 0 ||
        message.byteLength > WORKSPACE_FILE_MAX_BYTES
      ) {
        this.send({
          type: "workspace-file-write-result",
          requestId: message.requestId,
          outcome: "too-large",
          maxBytes: WORKSPACE_FILE_MAX_BYTES,
        });
        return;
      }
      if (this.writes.has(message.transferId)) {
        throw new Error("duplicate workspace file transfer id");
      }
      const filePath = resolveFilePath(message.path);
      const currentEtag = workspaceFileEtag(await statOrNull(filePath));
      if (currentEtag !== message.ifMatch) {
        this.send({
          type: "workspace-file-write-result",
          requestId: message.requestId,
          outcome: "conflict",
          etag: currentEtag,
        });
        return;
      }
      const temporaryPath = path.join(
        path.dirname(filePath),
        `.${path.basename(filePath)}.codex-web-save-${randomUUID()}.tmp`,
      );
      const handle = await fs.open(temporaryPath, "wx");
      this.writes.set(message.transferId, {
        byteLength: message.byteLength,
        expectedEtag: message.ifMatch,
        filePath,
        handle,
        position: 0,
        requestId: message.requestId,
        temporaryPath,
        transferId: message.transferId,
      });
      this.send({
        type: "workspace-file-write-ready",
        requestId: message.requestId,
        transferId: message.transferId,
      });
    } catch (error) {
      this.send({
        type: "workspace-file-write-error",
        requestId: message.requestId,
        errorMessage: errorMessage(error),
      });
    }
  }

  private async writeChunk(
    message: Extract<
      WorkspaceFileRendererMessage,
      { type: "workspace-file-write-chunk" }
    >,
  ): Promise<void> {
    const transfer = this.writes.get(message.transferId);
    try {
      if (!transfer || transfer.requestId !== message.requestId) {
        throw new Error("workspace file transfer not found");
      }
      if (
        message.offset !== transfer.position ||
        message.bytes.byteLength > WORKSPACE_FILE_CHUNK_BYTES ||
        transfer.position + message.bytes.byteLength > transfer.byteLength
      ) {
        throw new Error("invalid workspace file chunk");
      }
      await writeFully(transfer.handle, message.bytes, transfer.position);
      transfer.position += message.bytes.byteLength;
      this.send({
        type: "workspace-file-write-chunk-result",
        requestId: message.requestId,
        transferId: message.transferId,
        offset: message.offset,
        ok: true,
      });
    } catch (error) {
      await this.abortWrite(message.transferId);
      this.send({
        type: "workspace-file-write-chunk-result",
        requestId: message.requestId,
        transferId: message.transferId,
        offset: message.offset,
        ok: false,
        errorMessage: errorMessage(error),
      });
    }
  }

  private async commitWrite(
    message: Extract<
      WorkspaceFileRendererMessage,
      { type: "workspace-file-write-commit" }
    >,
  ): Promise<void> {
    const transfer = this.writes.get(message.transferId);
    if (!transfer || transfer.requestId !== message.requestId) {
      this.send({
        type: "workspace-file-write-error",
        requestId: message.requestId,
        errorMessage: "workspace file transfer not found",
      });
      return;
    }
    this.writes.delete(message.transferId);
    let handleOpen = true;
    try {
      if (transfer.position !== transfer.byteLength) {
        throw new Error("workspace file transfer is incomplete");
      }
      await transfer.handle.sync();
      await transfer.handle.close();
      handleOpen = false;
      const result = await withPathLock(transfer.filePath, async () => {
        const currentEtag = workspaceFileEtag(
          await statOrNull(transfer.filePath),
        );
        if (currentEtag !== transfer.expectedEtag) {
          return { outcome: "conflict" as const, etag: currentEtag };
        }
        await fs.rename(transfer.temporaryPath, transfer.filePath);
        return {
          outcome: "saved" as const,
          etag: workspaceFileEtag(await fs.stat(transfer.filePath)),
        };
      });
      if (result.outcome === "conflict") {
        await fs.rm(transfer.temporaryPath, { force: true });
      }
      this.send({
        type: "workspace-file-write-result",
        requestId: message.requestId,
        ...result,
      });
    } catch (error) {
      if (handleOpen) {
        await transfer.handle.close().catch(() => {});
      }
      await fs.rm(transfer.temporaryPath, { force: true }).catch(() => {});
      this.send({
        type: "workspace-file-write-error",
        requestId: message.requestId,
        errorMessage: errorMessage(error),
      });
    }
  }

  private async abortWrite(transferId: string): Promise<void> {
    const transfer = this.writes.get(transferId);
    if (!transfer) {
      return;
    }
    this.writes.delete(transferId);
    await transfer.handle.close().catch(() => {});
    await fs.rm(transfer.temporaryPath, { force: true }).catch(() => {});
  }

  private sendAndWait(message: WorkspaceFileMainMessage): Promise<void> {
    return Promise.resolve(this.send(message, true));
  }
}

export function workspaceFileEtag(
  stat: { mtimeMs: number; ctimeMs: number; size: number } | null,
): string {
  if (!stat) {
    return "missing";
  }
  return `stat:${createHash("sha256")
    .update(`${stat.mtimeMs}:${stat.ctimeMs}:${stat.size}`)
    .digest("base64url")}`;
}

function assertLocalHost(hostId: string): void {
  if (hostId !== "local") {
    throw new Error(`workspace files are not supported for host ${hostId}`);
  }
}

function resolveFilePath(value: string): string {
  if (!value.trim()) {
    throw new Error("workspace file path is required");
  }
  return path.resolve(value);
}

async function statOrNull(filePath: string) {
  try {
    return await fs.stat(filePath);
  } catch (error) {
    if (isNodeError(error) && error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function resolveRepresentation(
  handle: FileHandle,
  size: number,
  requested: "auto" | "blob" | "text",
): Promise<"blob" | "text"> {
  if (requested !== "auto") {
    return requested;
  }
  const buffer = new Uint8Array(Math.min(4096, size));
  const { bytesRead } = await handle.read(buffer, 0, buffer.byteLength, 0);
  const preview = buffer.subarray(0, bytesRead);
  if (preview.includes(0)) {
    return "blob";
  }
  try {
    new TextDecoder("utf-8", { fatal: true }).decode(preview, { stream: true });
    return "text";
  } catch {
    return "blob";
  }
}

async function writeFully(
  handle: FileHandle,
  bytes: Uint8Array,
  position: number,
): Promise<void> {
  let written = 0;
  while (written < bytes.byteLength) {
    const result = await handle.write(
      bytes,
      written,
      bytes.byteLength - written,
      position + written,
    );
    if (result.bytesWritten === 0) {
      throw new Error("failed to write workspace file chunk");
    }
    written += result.bytesWritten;
  }
}

async function withPathLock<T>(
  filePath: string,
  operation: () => Promise<T>,
): Promise<T> {
  const previous = pathLocks.get(filePath) ?? Promise.resolve();
  let release!: () => void;
  const current = new Promise<void>((resolve) => {
    release = resolve;
  });
  const queued = previous.then(() => current);
  pathLocks.set(filePath, queued);
  await previous;
  try {
    return await operation();
  } finally {
    release();
    if (pathLocks.get(filePath) === queued) {
      pathLocks.delete(filePath);
    }
  }
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) {
    throw new Error("workspace file read aborted");
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isNodeError(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && "code" in error;
}
