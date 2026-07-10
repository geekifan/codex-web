import {
  mapBrowserPathToInitialRoute,
  mapMemoryPathToBrowserPath,
} from "./routes";
import {
  handleLocalFilePickerMessage,
  isLocalFilePickerMessage,
} from "./files";
import {
  openSelectWorkspaceRootDialog,
  type WorkspaceDirectoryEntries,
} from "./workspace-root-dialog";
import {
  BRIDGE_HEADER_LENGTH,
  BRIDGE_PROTOCOL_VERSION,
  BridgeFrameType,
  decodeBridgeFrame,
  encodeBridgeFrame,
  encodeBridgeFramePayload,
  encodeBridgeValue,
  type BridgeFrame,
} from "../shared/bridge-protocol";
import {
  WORKSPACE_FILE_CHUNK_BYTES,
  type WorkspaceFileMainMessage,
  type WorkspaceFileRendererMessage,
  type WorkspaceFileRepresentation,
} from "../shared/workspace-files";

type IpcListener = (event: unknown, ...args: unknown[]) => void;

type RendererToMainMessage =
  | {
      type: "ipc-renderer-invoke";
      requestId: string;
      channel: string;
      args: unknown[];
    }
  | {
      type: "ipc-renderer-send";
      channel: string;
      args: unknown[];
    }
  | {
      type: "workspace-directory-entries-request";
      requestId: string;
      directoryPath: string | null;
      directoriesOnly: boolean;
    }
  | WorkspaceFileRendererMessage;

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
  | WorkspaceFileMainMessage;

type OutgoingBridgeMessage = {
  id: number;
  payload: Uint8Array;
  byteLength: number;
};

const RECONNECT_DELAY_MS = 1_000;
const SOCKET_TIMEOUT_MS = 20_000;
const MAX_UNACKED_BYTES = 64 * 1024 * 1_024;
const MAX_IN_FLIGHT_BYTES = 256 * 1024;

type MemoryNavigationChange = {
  action: "POP" | "PUSH" | "REPLACE";
  delta: number;
  location: {
    hash: string;
    key: string;
    pathname: string;
    search: string;
    state: unknown;
  };
};

type ElectronShimState = {
  initialRoute?: string;
  initialSidebarState?: boolean;
  closeSidebar?: () => void;
  services?: {
    workspaceFiles?: {
      read?: (args: {
        hostId: string;
        path: string;
        representation: WorkspaceFileRepresentation;
      }) => Promise<{ etag: string; text?: string; blob?: string }>;
      writeIfMatch?: (args: {
        bytes: Uint8Array;
        hostId: string;
        ifMatch: string;
        path: string;
      }) => Promise<
        | { outcome: "saved"; etag: string }
        | { outcome: "conflict"; etag: string }
        | { outcome: "too-large"; maxBytes: number }
      >;
    };
    requestUserInputAutoResolution?: {
      recordConversationActivity?: (args: {
        conversationId: string;
        hostId: string;
      }) => void;
      setConversationPresented?: (args: {
        conversationId: string;
        hostId: string;
        presented: boolean;
      }) => void;
      snooze?: (args: {
        conversationId: string;
        hostId: string;
        requestId: string;
      }) => void;
    };
  };
  onMemoryNavigationChanged?: (navigation: MemoryNavigationChange) => void;
  overrideAdapter?: {
    getGateOverride?: (
      e: StatsigGateEvaluation,
      ...args: unknown[]
    ) => StatsigGateEvaluation | null;
  };
};

type StatsigGateEvaluation = {
  name: string;
  value: boolean;
  [key: string]: unknown;
};

declare global {
  interface Window {
    __ELECTRON_SHIM__?: ElectronShimState;
  }
}

declare const __CODEX_APP_VERSION__: string;

let requestCounter = 0;
let socket: WebSocket | null = null;
let reconnectTimeoutId: number | null = null;
let socketTimeoutId: number | null = null;
let socketReady = false;
let lastIncomingAt = Date.now();
let serverEpoch: string | null = null;
let outgoingMessageId = 0;
let outgoingAckId = 0;
let outgoingSentId = 0;
let outgoingUnackedBytes = 0;
let incomingMessageId = 0;
const connectionId = crypto.randomUUID();
let outgoingUnacked: OutgoingBridgeMessage[] = [];
const pendingInvokes = new Map<
  string,
  {
    reject: (reason?: unknown) => void;
    resolve: (value: unknown) => void;
  }
>();
const pendingDirectoryEntries = new Map<
  string,
  {
    reject: (reason?: unknown) => void;
    resolve: (value: WorkspaceDirectoryEntries) => void;
  }
>();
type PendingWorkspaceRead = {
  byteLength?: number;
  chunks: Uint8Array[];
  etag?: string;
  expectedOffset: number;
  reject: (reason?: unknown) => void;
  representation?: "blob" | "text";
  resolve: (value: { etag: string; text?: string; blob?: string }) => void;
  transferId?: string;
};
type PendingWorkspaceWrite = {
  reject: (reason?: unknown) => void;
  resolve: (
    value:
      | { outcome: "saved"; etag: string }
      | { outcome: "conflict"; etag: string }
      | { outcome: "too-large"; maxBytes: number },
  ) => void;
  ready?: () => void;
  readyReject?: (reason?: unknown) => void;
  chunk?: {
    offset: number;
    resolve: () => void;
    reject: (reason?: unknown) => void;
  };
  transferId: string;
};
const pendingWorkspaceReads = new Map<string, PendingWorkspaceRead>();
const pendingWorkspaceWrites = new Map<string, PendingWorkspaceWrite>();
const rendererListeners = new Map<string, Set<IpcListener>>();

function unimplemented(method: string): never {
  debugger;
  throw new Error(`[electron-stub] ${method} is not implemented`);
}

export function emitRendererEvent(channel: string, args: unknown[]): void {
  const listeners = rendererListeners.get(channel);
  if (!listeners || listeners.size === 0) {
    return;
  }
  const event = { sender: null };
  for (const listener of listeners) {
    listener(event, ...args);
  }
}

function handleIncomingMessage(message: MainToRendererMessage): void {
  if (message.type === "workspace-file-read-start") {
    const pending = pendingWorkspaceReads.get(message.requestId);
    if (!pending || pending.transferId) {
      return;
    }
    pending.transferId = message.transferId;
    pending.etag = message.etag;
    pending.byteLength = message.byteLength;
    pending.representation = message.representation;
    return;
  }

  if (message.type === "workspace-file-read-chunk") {
    const pending = pendingWorkspaceReads.get(message.requestId);
    if (
      !pending ||
      pending.transferId !== message.transferId ||
      pending.expectedOffset !== message.offset
    ) {
      return;
    }
    pending.chunks.push(message.bytes);
    pending.expectedOffset += message.bytes.byteLength;
    return;
  }

  if (message.type === "workspace-file-read-end") {
    const pending = pendingWorkspaceReads.get(message.requestId);
    if (
      !pending ||
      pending.transferId !== message.transferId ||
      pending.etag == null ||
      pending.representation == null ||
      pending.expectedOffset !== pending.byteLength
    ) {
      pending?.reject(new Error("invalid workspace file read result"));
      pendingWorkspaceReads.delete(message.requestId);
      return;
    }
    pendingWorkspaceReads.delete(message.requestId);
    if (pending.representation === "text") {
      const decoder = new TextDecoder("utf-8");
      let text = "";
      for (const chunk of pending.chunks) {
        text += decoder.decode(chunk, { stream: true });
      }
      text += decoder.decode();
      pending.resolve({ etag: pending.etag, text });
      return;
    }
    pending.resolve({
      etag: pending.etag,
      blob: bytesToBase64(pending.chunks),
    });
    return;
  }

  if (message.type === "workspace-file-read-error") {
    const pending = pendingWorkspaceReads.get(message.requestId);
    if (!pending) {
      return;
    }
    pendingWorkspaceReads.delete(message.requestId);
    pending.reject(new Error(message.errorMessage));
    return;
  }

  if (message.type === "workspace-file-write-ready") {
    const pending = pendingWorkspaceWrites.get(message.requestId);
    if (pending?.transferId === message.transferId) {
      pending.ready?.();
    }
    return;
  }

  if (message.type === "workspace-file-write-chunk-result") {
    const pending = pendingWorkspaceWrites.get(message.requestId);
    const chunk = pending?.chunk;
    if (
      !pending ||
      pending.transferId !== message.transferId ||
      !chunk ||
      chunk.offset !== message.offset
    ) {
      return;
    }
    pending.chunk = undefined;
    if (message.ok) {
      chunk.resolve();
    } else {
      chunk.reject(new Error(message.errorMessage));
    }
    return;
  }

  if (message.type === "workspace-file-write-result") {
    const pending = pendingWorkspaceWrites.get(message.requestId);
    if (!pending) {
      return;
    }
    pendingWorkspaceWrites.delete(message.requestId);
    pending.resolve(
      message.outcome === "too-large"
        ? { outcome: message.outcome, maxBytes: message.maxBytes }
        : { outcome: message.outcome, etag: message.etag },
    );
    pending.readyReject?.(new Error("workspace file write completed"));
    return;
  }

  if (message.type === "workspace-file-write-error") {
    const pending = pendingWorkspaceWrites.get(message.requestId);
    if (!pending) {
      return;
    }
    pendingWorkspaceWrites.delete(message.requestId);
    const error = new Error(message.errorMessage);
    pending.readyReject?.(error);
    pending.chunk?.reject(error);
    pending.reject(error);
    return;
  }

  if (message.type === "ipc-main-event") {
    emitRendererEvent(message.channel, message.args);
    return;
  }

  if (message.type === "ipc-renderer-invoke-result") {
    const pending = pendingInvokes.get(message.requestId);
    if (!pending) {
      return;
    }
    pendingInvokes.delete(message.requestId);
    if (message.ok) {
      pending.resolve(message.result);
      return;
    }
    pending.reject(new Error(message.errorMessage));
    return;
  }

  if (message.type === "workspace-directory-entries-result") {
    const pending = pendingDirectoryEntries.get(message.requestId);
    if (!pending) {
      return;
    }
    pendingDirectoryEntries.delete(message.requestId);
    if (message.ok) {
      pending.resolve(message.result);
      return;
    }
    pending.reject(new Error(message.errorMessage));
  }
}

function scheduleReconnect(): void {
  if (reconnectTimeoutId !== null) {
    return;
  }
  reconnectTimeoutId = window.setTimeout(() => {
    reconnectTimeoutId = null;
    ensureSocket();
  }, RECONNECT_DELAY_MS);
}

function ensureSocket(): void {
  if (
    socket &&
    (socket.readyState === WebSocket.OPEN ||
      socket.readyState === WebSocket.CONNECTING)
  ) {
    return;
  }

  const nextSocket = new WebSocket(
    `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/__backend/ipc`,
  );
  nextSocket.binaryType = "arraybuffer";
  socket = nextSocket;
  socketReady = false;
  nextSocket.addEventListener("open", () => {
    if (socket !== nextSocket) {
      return;
    }
    lastIncomingAt = Date.now();
    sendFrame(BridgeFrameType.Control, 0, 0, {
      type: "bridge-hello",
      protocolVersion: BRIDGE_PROTOCOL_VERSION,
      connectionId,
      serverEpoch,
    });
    startSocketTimeout(nextSocket);
  });
  nextSocket.addEventListener("message", (event) => {
    if (socket !== nextSocket) {
      return;
    }
    lastIncomingAt = Date.now();
    try {
      if (!(event.data instanceof ArrayBuffer)) {
        throw new Error("binary bridge frame required");
      }
      const frame = decodeBridgeFrame(new Uint8Array(event.data));
      handleBridgeFrame(frame);
    } catch (error) {
      console.error(
        "[electron-stub] failed to parse IPC bridge message",
        error,
      );
      resetBridge("invalid reliable bridge frame");
    }
  });
  nextSocket.addEventListener("close", () => {
    if (socket !== nextSocket) {
      return;
    }
    stopSocketTimeout();
    socket = null;
    socketReady = false;
    scheduleReconnect();
  });
  nextSocket.addEventListener("error", () => {
    if (socket === nextSocket) {
      nextSocket.close();
    }
  });
}

function enqueueMessage(message: RendererToMainMessage): void {
  let payload: Uint8Array;
  try {
    payload = encodeBridgeValue(message);
  } catch (error) {
    console.error("[electron-stub] failed to encode IPC bridge message", error);
    resetBridge("failed to encode reliable bridge message");
    return;
  }
  const outgoing = {
    id: ++outgoingMessageId,
    payload,
    byteLength: BRIDGE_HEADER_LENGTH + payload.byteLength,
  };
  outgoingUnacked.push(outgoing);
  outgoingUnackedBytes += outgoing.byteLength;
  if (outgoingUnackedBytes > MAX_UNACKED_BYTES) {
    resetBridge("reliable bridge buffer exceeded");
    return;
  }
  ensureSocket();
  pumpOutgoing();
}

function handleBridgeFrame(frame: BridgeFrame): void {
  if (frame.type === BridgeFrameType.Control) {
    if (!isRecord(frame.payload) || typeof frame.payload.type !== "string") {
      resetBridge("invalid reliable bridge control message");
      return;
    }
    if (frame.payload.type === "bridge-reset") {
      resetBridge(
        typeof frame.payload.reason === "string"
          ? frame.payload.reason
          : "reliable bridge reset",
      );
      return;
    }
    if (frame.payload.type !== "bridge-ready") {
      resetBridge("unsupported reliable bridge control message");
      return;
    }
    if (frame.payload.connectionId !== connectionId) {
      resetBridge("reliable bridge connection mismatch");
      return;
    }
    if (
      typeof frame.payload.serverEpoch !== "string" ||
      (serverEpoch !== null && serverEpoch !== frame.payload.serverEpoch)
    ) {
      resetBridge("backend restarted");
      return;
    }
    serverEpoch = frame.payload.serverEpoch;
    socketReady = true;
    sendAck();
    outgoingSentId = outgoingAckId;
    pumpOutgoing();
    return;
  }

  if (frame.type === BridgeFrameType.Regular) {
    if (!acceptAck(frame.ack)) {
      return;
    }
    acceptMessage(frame.id, frame.payload);
    return;
  }

  if (frame.type === BridgeFrameType.Ack) {
    acceptAck(frame.ack);
    return;
  }

  if (frame.type === BridgeFrameType.ReplayRequest) {
    if (acceptAck(frame.ack, false)) {
      outgoingSentId = frame.ack;
      pumpOutgoing();
    }
    return;
  }

  if (frame.type === BridgeFrameType.KeepAlive) {
    sendFrame(BridgeFrameType.KeepAlive, 0, incomingMessageId);
    return;
  }

  resetBridge("unsupported reliable bridge frame");
}

function acceptMessage(id: number, message: unknown): void {
  if (!Number.isSafeInteger(id) || id <= 0 || !isRecord(message)) {
    resetBridge("invalid reliable bridge message id");
    return;
  }

  if (id === incomingMessageId + 1) {
    incomingMessageId = id;
    handleIncomingMessage(message as MainToRendererMessage);
    sendAck();
    return;
  }

  if (id <= incomingMessageId) {
    sendAck();
    return;
  }

  sendFrame(BridgeFrameType.ReplayRequest, 0, incomingMessageId);
}

function acceptAck(ack: number, pump = true): boolean {
  if (!Number.isSafeInteger(ack) || ack < 0 || ack > outgoingMessageId) {
    resetBridge("invalid reliable bridge acknowledgement");
    return false;
  }
  if (ack <= outgoingAckId) {
    return true;
  }

  outgoingAckId = ack;
  const acknowledged = outgoingUnacked.filter((message) => message.id <= ack);
  outgoingUnackedBytes -= acknowledged.reduce(
    (total, message) => total + message.byteLength,
    0,
  );
  outgoingUnacked = outgoingUnacked.filter((message) => message.id > ack);
  if (pump) {
    pumpOutgoing();
  }
  return true;
}

function pumpOutgoing(): void {
  if (!socketReady) {
    return;
  }

  let inFlightBytes = outgoingUnacked
    .filter((message) => message.id <= outgoingSentId)
    .reduce((total, message) => total + message.byteLength, 0);
  for (const message of outgoingUnacked) {
    if (message.id <= outgoingSentId) {
      continue;
    }
    if (
      inFlightBytes > 0 &&
      inFlightBytes + message.byteLength > MAX_IN_FLIGHT_BYTES
    ) {
      break;
    }
    sendData(message);
    outgoingSentId = message.id;
    inFlightBytes += message.byteLength;
  }
}

function sendData(message: OutgoingBridgeMessage): void {
  if (!socketReady) {
    return;
  }
  sendEncodedFrame(
    BridgeFrameType.Regular,
    message.id,
    incomingMessageId,
    message.payload,
  );
}

function sendAck(): void {
  if (!socketReady) {
    return;
  }
  sendFrame(BridgeFrameType.Ack, 0, incomingMessageId);
}

function sendFrame(
  type: (typeof BridgeFrameType)[keyof typeof BridgeFrameType],
  id: number,
  ack: number,
  payload?: unknown,
): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  try {
    socket.send(encodeBridgeFrame({ type, id, ack, payload }));
  } catch {
    socket.close();
  }
}

function sendEncodedFrame(
  type: (typeof BridgeFrameType)[keyof typeof BridgeFrameType],
  id: number,
  ack: number,
  payload: Uint8Array,
): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  try {
    socket.send(encodeBridgeFramePayload(type, id, ack, payload));
  } catch {
    socket.close();
  }
}

function startSocketTimeout(currentSocket: WebSocket): void {
  stopSocketTimeout();
  socketTimeoutId = window.setInterval(() => {
    if (
      socket === currentSocket &&
      Date.now() - lastIncomingAt > SOCKET_TIMEOUT_MS
    ) {
      currentSocket.close();
    }
  }, 5_000);
}

function stopSocketTimeout(): void {
  if (socketTimeoutId !== null) {
    window.clearInterval(socketTimeoutId);
    socketTimeoutId = null;
  }
}

function resetBridge(reason: string): void {
  console.error(`[electron-stub] reliable IPC bridge reset: ${reason}`);
  stopSocketTimeout();
  socket?.close();
  window.location.reload();
}

function nextRequestId(): string {
  requestCounter += 1;
  return `ipc_bridge_${requestCounter}`;
}

function invokeMain(channel: string, args: unknown[]): Promise<unknown> {
  const requestId = nextRequestId();
  return new Promise((resolve, reject) => {
    pendingInvokes.set(requestId, { resolve, reject });
    enqueueMessage({
      type: "ipc-renderer-invoke",
      requestId,
      channel,
      args,
    });
  });
}

function addIpcListener(channel: string, listener: IpcListener): void {
  const listeners = rendererListeners.get(channel) ?? new Set<IpcListener>();
  listeners.add(listener);
  rendererListeners.set(channel, listeners);
}

function shouldCloseSidebarForMemoryPath(path: string): boolean {
  return (
    path === "/" ||
    path.startsWith("/local/") ||
    path === "/skills" ||
    path === "/automations"
  );
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isUnhandledAddWorkspaceRootOptionMessage(value: unknown): value is {
  root?: unknown;
  type: "electron-add-new-workspace-root-option";
} {
  return (
    isRecord(value) &&
    value.type === "electron-add-new-workspace-root-option" &&
    typeof value.root !== "string"
  );
}

function isOpenInBrowserMessage(value: unknown): value is {
  type: "open-in-browser";
  url: string;
} {
  return (
    isRecord(value) &&
    value.type === "open-in-browser" &&
    typeof value.url === "string"
  );
}

function requestWorkspaceDirectoryEntries(
  directoryPath: string | null,
): Promise<WorkspaceDirectoryEntries> {
  const requestId = nextRequestId();
  return new Promise((resolve, reject) => {
    pendingDirectoryEntries.set(requestId, { resolve, reject });
    enqueueMessage({
      type: "workspace-directory-entries-request",
      requestId,
      directoryPath,
      directoriesOnly: true,
    });
  });
}

function readWorkspaceFile(args: {
  hostId: string;
  path: string;
  representation: WorkspaceFileRepresentation;
}): Promise<{ etag: string; text?: string; blob?: string }> {
  if (args.hostId !== "local") {
    return Promise.reject(
      new Error(`workspace files are not supported for host ${args.hostId}`),
    );
  }
  const requestId = nextRequestId();
  return new Promise((resolve, reject) => {
    pendingWorkspaceReads.set(requestId, {
      chunks: [],
      expectedOffset: 0,
      reject,
      resolve,
    });
    enqueueMessage({
      type: "workspace-file-read-request",
      requestId,
      ...args,
    });
  });
}

function writeWorkspaceFileIfMatch(args: {
  bytes: Uint8Array;
  hostId: string;
  ifMatch: string;
  path: string;
}): Promise<
  | { outcome: "saved"; etag: string }
  | { outcome: "conflict"; etag: string }
  | { outcome: "too-large"; maxBytes: number }
> {
  if (args.hostId !== "local") {
    return Promise.reject(
      new Error(`workspace files are not supported for host ${args.hostId}`),
    );
  }
  const requestId = nextRequestId();
  const transferId = crypto.randomUUID();
  return new Promise((resolve, reject) => {
    const pending: PendingWorkspaceWrite = {
      reject,
      resolve,
      transferId,
    };
    pendingWorkspaceWrites.set(requestId, pending);
    void (async () => {
      try {
        await new Promise<void>((readyResolve, readyReject) => {
          pending.ready = readyResolve;
          pending.readyReject = readyReject;
          enqueueMessage({
            type: "workspace-file-write-start",
            requestId,
            transferId,
            hostId: args.hostId,
            path: args.path,
            ifMatch: args.ifMatch,
            byteLength: args.bytes.byteLength,
          });
        });
        pending.ready = undefined;
        pending.readyReject = undefined;

        for (
          let offset = 0;
          offset < args.bytes.byteLength;
          offset += WORKSPACE_FILE_CHUNK_BYTES
        ) {
          const bytes = args.bytes.subarray(
            offset,
            Math.min(
              offset + WORKSPACE_FILE_CHUNK_BYTES,
              args.bytes.byteLength,
            ),
          );
          await new Promise<void>((chunkResolve, chunkReject) => {
            pending.chunk = {
              offset,
              resolve: chunkResolve,
              reject: chunkReject,
            };
            enqueueMessage({
              type: "workspace-file-write-chunk",
              requestId,
              transferId,
              offset,
              bytes,
            });
          });
        }
        enqueueMessage({
          type: "workspace-file-write-commit",
          requestId,
          transferId,
        });
      } catch (error) {
        if (!pendingWorkspaceWrites.has(requestId)) {
          return;
        }
        pendingWorkspaceWrites.delete(requestId);
        enqueueMessage({
          type: "workspace-file-write-abort",
          transferId,
        });
        reject(error);
      }
    })();
  });
}

function bytesToBase64(chunks: Uint8Array[]): string {
  let result = "";
  let remainder = new Uint8Array();
  for (const chunk of chunks) {
    const combined = new Uint8Array(remainder.byteLength + chunk.byteLength);
    combined.set(remainder);
    combined.set(chunk, remainder.byteLength);
    const completeLength = combined.byteLength - (combined.byteLength % 3);
    if (completeLength > 0) {
      result += binaryBytesToBase64(combined.subarray(0, completeLength));
    }
    remainder = combined.slice(completeLength);
  }
  if (remainder.byteLength > 0) {
    result += binaryBytesToBase64(remainder);
  }
  return result;
}

function binaryBytesToBase64(bytes: Uint8Array): string {
  const parts: string[] = [];
  for (let offset = 0; offset < bytes.byteLength; offset += 0x8000) {
    const end = Math.min(offset + 0x8000, bytes.byteLength);
    let part = "";
    for (let index = offset; index < end; index += 1) {
      part += String.fromCharCode(bytes[index]!);
    }
    parts.push(part);
  }
  return btoa(parts.join(""));
}

const themeMediaQuery = matchMedia("(prefers-color-scheme: dark)");
const mobileMediaQuery = matchMedia("(max-width: 768px)");
const initialSidebarState = !mobileMediaQuery.matches;
const electronShim = (window.__ELECTRON_SHIM__ ??= {});

Object.assign(globalThis, {
  process: {
    arch: "arm64",
    platform: "darwin",
    versions: {
      electron: "41.2.0",
    },
  },
});

electronShim.services = {
  ...electronShim.services,
  workspaceFiles: {
    ...electronShim.services?.workspaceFiles,
    read: readWorkspaceFile,
    writeIfMatch: writeWorkspaceFileIfMatch,
  },
  requestUserInputAutoResolution: {
    ...electronShim.services?.requestUserInputAutoResolution,
    recordConversationActivity: () => undefined,
    setConversationPresented: () => undefined,
    snooze: () => undefined,
  },
};

electronShim.overrideAdapter = {
  getGateOverride(e) {
    if (e.name === "2929582856") {
      // codex_app_sunset
      return {
        ...e,
        value: false,
      };
    }

    if (e.name === "2478676115") {
      // Profile Selector
      return {
        ...e,
        value: true,
      };
    }

    return null;
  },
};

const initialRoute = mapBrowserPathToInitialRoute(
  window.location.pathname,
  window.location.search,
);
electronShim.initialRoute = initialRoute.memoryPath;

if (initialRoute.browserPath) {
  window.history.pushState(undefined, "", initialRoute.browserPath);
}

electronShim.initialSidebarState = initialSidebarState;
electronShim.onMemoryNavigationChanged = (navigation) => {
  const path = navigation.location.pathname;
  if (
    navigation.action !== "POP" &&
    mobileMediaQuery.matches &&
    shouldCloseSidebarForMemoryPath(path)
  ) {
    electronShim.closeSidebar?.();
  }

  const browserPath = mapMemoryPathToBrowserPath(path);
  if (browserPath == null) {
    return;
  }

  if (browserPath.titleChange) {
    document.title = browserPath.titleChange;
  }

  if (window.location.pathname === browserPath.path) {
    window.history.replaceState(undefined, "", browserPath.path);
    return;
  }

  window.history.pushState(undefined, "", browserPath.path);
};

const buildFlavor: "prod" | "dev" | "agent" | string = "prod";

export const ipcRenderer = {
  invoke(channel: string, ...args: unknown[]): Promise<unknown> {
    if (channel === "codex_desktop:message-from-view" && args.length === 1) {
      if (isOpenInBrowserMessage(args[0])) {
        window.open(args[0].url, "_blank", "noopener,noreferrer");
      }

      if (isLocalFilePickerMessage(args[0])) {
        return handleLocalFilePickerMessage(args[0]);
      }

      if (isUnhandledAddWorkspaceRootOptionMessage(args[0])) {
        return openSelectWorkspaceRootDialog({
          listDirectory: requestWorkspaceDirectoryEntries,
        }).then((root) => {
          if (!root) {
            return undefined;
          }

          return invokeMain(channel, [{ ...args[0], root }]);
        });
      }
    }

    return invokeMain(channel, args);
  },
  on(channel: string, listener: IpcListener): unknown {
    addIpcListener(channel, listener);
    return this;
  },
  once(channel: string, listener: IpcListener): unknown {
    const wrapped: IpcListener = (event, ...args) => {
      this.removeListener(channel, wrapped);
      listener(event, ...args);
    };
    addIpcListener(channel, wrapped);
    return this;
  },
  addListener(channel: string, listener: IpcListener): unknown {
    addIpcListener(channel, listener);
    return this;
  },
  removeListener(channel: string, listener: IpcListener): unknown {
    rendererListeners.get(channel)?.delete(listener);
    return this;
  },
  off(channel: string, listener: IpcListener): unknown {
    return this.removeListener(channel, listener);
  },
  send(channel: string, ...args: unknown[]): void {
    enqueueMessage({
      type: "ipc-renderer-send",
      channel,
      args,
    });
  },
  postMessage(
    channel: string,
    message: unknown,
    transfer?: Transferable[],
  ): void {
    if (transfer && transfer.length > 0) {
      return;
    }

    enqueueMessage({
      type: "ipc-renderer-send",
      channel,
      args: [message],
    });
  },
  sendSync(channel: string, ..._args: unknown[]): unknown {
    if (channel === "codex_desktop:get-sentry-init-options") {
      return {
        codexAppSessionId: "42626fde-7064-471f-b44d-b1a7ad849c7f",
        buildFlavor,
        buildNumber: null,
        appVersion: __CODEX_APP_VERSION__,
        enabled: false,
      };
    }

    if (channel === "codex_desktop:get-build-flavor") {
      return buildFlavor;
    }

    if (channel === "codex_desktop:get-uses-owl-app-shell") {
      return false;
    }

    if (channel === "codex_desktop:get-shared-object-snapshot") {
      return {
        host_config: { id: "local", display_name: "Local", kind: "local" },
        remote_ssh_connections: [],
        remote_wsl_connections: [],
        remote_control_connections_state: {
          available: false,
          accessRequired: false,
          authRequired: false,
          clientAuthorized: false,
        },
        local_remote_control_client_id: null,
        pending_worktrees: [],
      };
    }

    if (channel === "codex_desktop:get-system-theme-variant") {
      return themeMediaQuery.matches ? "dark" : "light";
    }

    return unimplemented("ipcRenderer.sendSync");
  },
};

ensureSocket();
window.addEventListener("pagehide", () => {
  if (socketReady) {
    sendFrame(BridgeFrameType.Disconnect, 0, incomingMessageId);
  }
});

export const contextBridge = {
  exposeInMainWorld(_key: string, _api: unknown): void {
    Reflect.set(window, _key, _api);
  },
};

export const webUtils = {
  getPathForFile(_file: File): string | null {
    return unimplemented("webUtils.getPathForFile");
  },
};
