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
    }
  | {
      type: "history-snapshots-request";
      requestId: string;
      request: HistorySnapshotsRequest;
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
    }
  | {
      type: "history-snapshots-result";
      requestId: string;
      ok: true;
      result: unknown;
    }
  | {
      type: "history-snapshots-result";
      requestId: string;
      ok: false;
      errorMessage: string;
    }
  | {
      type: "history-snapshots-lease-invalidated";
      subscriptionId: string;
    };

type WritableRoot = {
  kind: "local";
  path: string;
};

type ProjectWritableRootsResult = {
  changed: boolean;
  projectId: string;
  roots: WritableRoot[];
};

type AddRootRequest = {
  legacyRoot: string | null;
  projectId: string;
  root?: string;
};

type RemoveRootRequest = {
  legacyRoot: string | null;
  projectId: string;
  root: string;
};

type ClearRootsRequest = {
  legacyRoot: string | null;
  projectId: string;
};

type ThreadProjectAssignment = {
  projectId: string;
  projectKind: "local" | "remote";
  path?: string;
  cwd?: string;
  hostId?: string;
  pendingCoreUpdate: boolean;
};

type HistorySnapshotsRequest =
  | {
      operation: "acquireAuthorizationLease";
      hostId: string;
    }
  | {
      operation: "read" | "delete";
      hostId: string;
      authorizationLease: string;
      threadId: string;
    }
  | {
      operation: "write";
      hostId: string;
      authorizationLease: string;
      snapshotJson: string;
    }
  | {
      operation: "subscribeAuthorizationLeaseInvalidation";
      hostId: string;
      subscriptionId: string;
    }
  | {
      operation: "unsubscribeAuthorizationLeaseInvalidation";
      subscriptionId: string;
    };

type AppServerHistorySnapshotsService = {
  acquireAuthorizationLease: (hostId: string) => Promise<unknown>;
  delete: (
    hostId: string,
    authorizationLease: string,
    threadId: string,
  ) => Promise<unknown>;
  read: (
    hostId: string,
    authorizationLease: string,
    threadId: string,
  ) => Promise<unknown>;
  subscribeAuthorizationLeaseInvalidation: (
    hostId: string,
    callback: () => void,
  ) => Promise<{ unsubscribe: () => void }>;
  write: (
    hostId: string,
    authorizationLease: string,
    snapshotJson: string,
  ) => Promise<unknown>;
};

type AppHostServiceName =
  | "localEnvironments"
  | "pluginScheduledTasks"
  | "threadArchive"
  | "threadMetadataGeneration";

type AppHostService = Record<string, (...args: unknown[]) => Promise<unknown>>;

type BridgeServerFrame =
  | {
      type: "bridge-ready";
      connectionId: string;
      serverEpoch: string;
    }
  | {
      type: "bridge-data";
      id: number;
      ack: number;
      message: MainToRendererMessage;
    }
  | {
      type: "bridge-ack";
      ack: number;
    }
  | {
      type: "bridge-replay-request";
      ack: number;
    }
  | {
      type: "bridge-keepalive";
    }
  | {
      type: "bridge-reset";
      reason: string;
    };

type OutgoingBridgeMessage = {
  id: number;
  message: RendererToMainMessage;
  byteLength: number;
};

const BRIDGE_PROTOCOL_VERSION = 2;
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
    projectWritableRoots?: {
      addRoot?: (
        request: AddRootRequest,
      ) => Promise<ProjectWritableRootsResult>;
      clearRoots?: (
        request: ClearRootsRequest,
      ) => Promise<ProjectWritableRootsResult>;
      removeRoot?: (
        request: RemoveRootRequest,
      ) => Promise<ProjectWritableRootsResult>;
    };
    threadProjectAssignments?: {
      setAssignment?: (request: {
        threadId: string;
        assignment: ThreadProjectAssignment | null;
      }) => Promise<void>;
    };
    appServerHistorySnapshots?: AppServerHistorySnapshotsService;
    localEnvironments?: AppHostService;
    pluginScheduledTasks?: AppHostService;
    threadArchive?: AppHostService;
    threadMetadataGeneration?: AppHostService;
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
    getDynamicConfigOverride?: (
      e: StatsigDynamicConfigEvaluation,
      ...args: unknown[]
    ) => StatsigDynamicConfigEvaluation | null;
    getGateOverride?: (
      e: StatsigGateEvaluation,
      ...args: unknown[]
    ) => StatsigGateEvaluation | null;
    getLayerOverride?: (
      e: StatsigLayerEvaluation,
      ...args: unknown[]
    ) => StatsigLayerEvaluation | null;
  };
};

type StatsigGateEvaluation = {
  name: string;
  value: boolean;
  [key: string]: unknown;
};

type StatsigDynamicConfigEvaluation = {
  name: string;
  value: Record<string, unknown>;
  [key: string]: unknown;
};

type StatsigLayerEvaluation = {
  name: string;
  __value: Record<string, unknown>;
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
const pendingProjectWritableRoots = new Map<
  string,
  {
    reject: (reason?: unknown) => void;
    resolve: (result: ProjectWritableRootsResult) => void;
  }
>();
const pendingThreadProjectAssignments = new Map<
  string,
  {
    reject: (reason?: unknown) => void;
    resolve: () => void;
  }
>();
const pendingHistorySnapshots = new Map<
  string,
  {
    reject: (reason?: unknown) => void;
    resolve: (result: unknown) => void;
  }
>();
const historySnapshotInvalidationCallbacks = new Map<string, () => void>();
const rendererListeners = new Map<string, Set<IpcListener>>();
const reportedRendererListenerErrors = new Set<string>();

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
    try {
      listener(event, ...args);
    } catch (error) {
      if (!reportedRendererListenerErrors.has(channel)) {
        reportedRendererListenerErrors.add(channel);
        console.error(`[electron-stub] IPC listener failed for ${channel}`, error);
      }
    }
  }
}

function handleIncomingMessage(message: MainToRendererMessage): void {
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
    return;
  }

  if (message.type === "project-writable-roots-result") {
    const pending = pendingProjectWritableRoots.get(message.requestId);
    if (!pending) {
      return;
    }
    pendingProjectWritableRoots.delete(message.requestId);
    if (message.ok) {
      pending.resolve(message.result);
      return;
    }
    pending.reject(new Error(message.errorMessage));
    return;
  }

  if (message.type === "thread-project-assignment-result") {
    const pending = pendingThreadProjectAssignments.get(message.requestId);
    if (!pending) {
      return;
    }
    pendingThreadProjectAssignments.delete(message.requestId);
    if (message.ok) {
      pending.resolve();
      return;
    }
    pending.reject(new Error(message.errorMessage));
    return;
  }

  if (message.type === "history-snapshots-result") {
    const pending = pendingHistorySnapshots.get(message.requestId);
    if (!pending) {
      return;
    }
    pendingHistorySnapshots.delete(message.requestId);
    if (message.ok) {
      pending.resolve(message.result);
      return;
    }
    pending.reject(new Error(message.errorMessage));
    return;
  }

  if (message.type === "history-snapshots-lease-invalidated") {
    try {
      historySnapshotInvalidationCallbacks.get(message.subscriptionId)?.();
    } catch (error) {
      console.error(
        "[electron-stub] history snapshot invalidation callback failed",
        error,
      );
    }
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
  socket = nextSocket;
  socketReady = false;
  nextSocket.addEventListener("open", () => {
    if (socket !== nextSocket) {
      return;
    }
    lastIncomingAt = Date.now();
    sendRaw({
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
      const frame = JSON.parse(String(event.data)) as BridgeServerFrame;
      if (!frame || typeof frame !== "object" || !("type" in frame)) {
        resetBridge("invalid reliable bridge frame");
        return;
      }
      handleBridgeFrame(frame);
    } catch (error) {
      console.error(
        "[electron-stub] failed to parse IPC bridge message",
        error,
      );
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
  const byteLength = new TextEncoder().encode(JSON.stringify(message)).length;
  const outgoing = {
    id: ++outgoingMessageId,
    message,
    byteLength,
  };
  outgoingUnacked.push(outgoing);
  outgoingUnackedBytes += byteLength;
  if (outgoingUnackedBytes > MAX_UNACKED_BYTES) {
    resetBridge("reliable bridge buffer exceeded");
    return;
  }
  ensureSocket();
  pumpOutgoing();
}

function handleBridgeFrame(frame: BridgeServerFrame): void {
  if (frame.type === "bridge-ready") {
    if (frame.connectionId !== connectionId) {
      resetBridge("reliable bridge connection mismatch");
      return;
    }
    if (serverEpoch !== null && serverEpoch !== frame.serverEpoch) {
      resetBridge("backend restarted");
      return;
    }
    serverEpoch = frame.serverEpoch;
    socketReady = true;
    sendAck();
    outgoingSentId = outgoingAckId;
    pumpOutgoing();
    return;
  }

  if (frame.type === "bridge-reset") {
    resetBridge(frame.reason);
    return;
  }

  if (frame.type === "bridge-data") {
    if (!acceptAck(frame.ack)) {
      return;
    }
    acceptMessage(frame);
    return;
  }

  if (frame.type === "bridge-ack") {
    acceptAck(frame.ack);
    return;
  }

  if (frame.type === "bridge-replay-request") {
    if (acceptAck(frame.ack, false)) {
      outgoingSentId = frame.ack;
      pumpOutgoing();
    }
    return;
  }

  if (frame.type === "bridge-keepalive") {
    sendRaw({ type: "bridge-keepalive" });
    return;
  }

  resetBridge("unsupported reliable bridge frame");
}

function acceptMessage(
  frame: Extract<BridgeServerFrame, { type: "bridge-data" }>,
): void {
  if (!Number.isSafeInteger(frame.id) || frame.id <= 0) {
    resetBridge("invalid reliable bridge message id");
    return;
  }

  if (frame.id === incomingMessageId + 1) {
    incomingMessageId = frame.id;
    handleIncomingMessage(frame.message);
    sendAck();
    return;
  }

  if (frame.id <= incomingMessageId) {
    sendAck();
    return;
  }

  sendRaw({ type: "bridge-replay-request", ack: incomingMessageId });
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
  sendRaw({
    type: "bridge-data",
    id: message.id,
    ack: incomingMessageId,
    message: message.message,
  });
}

function sendAck(): void {
  if (!socketReady) {
    return;
  }
  sendRaw({ type: "bridge-ack", ack: incomingMessageId });
}

function sendRaw(frame: object): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  try {
    socket.send(JSON.stringify(frame));
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

function requestProjectWritableRoots(
  operation: "addRoot" | "clearRoots" | "removeRoot",
  request: AddRootRequest | ClearRootsRequest | RemoveRootRequest,
): Promise<ProjectWritableRootsResult> {
  const requestId = nextRequestId();
  return new Promise((resolve, reject) => {
    pendingProjectWritableRoots.set(requestId, { resolve, reject });
    enqueueMessage({
      type: "project-writable-roots-request",
      requestId,
      operation,
      ...request,
    });
  });
}

function requestThreadProjectAssignment(request: {
  threadId: string;
  assignment: ThreadProjectAssignment | null;
}): Promise<void> {
  const requestId = nextRequestId();
  return new Promise((resolve, reject) => {
    pendingThreadProjectAssignments.set(requestId, { resolve, reject });
    enqueueMessage({
      type: "thread-project-assignment-request",
      requestId,
      ...request,
    });
  });
}

function requestHistorySnapshots(
  request: HistorySnapshotsRequest,
): Promise<unknown> {
  const requestId = nextRequestId();
  return new Promise((resolve, reject) => {
    pendingHistorySnapshots.set(requestId, { resolve, reject });
    enqueueMessage({
      type: "history-snapshots-request",
      requestId,
      request,
    });
  });
}

const appServerHistorySnapshots: AppServerHistorySnapshotsService = {
  acquireAuthorizationLease: (hostId) =>
    requestHistorySnapshots({ operation: "acquireAuthorizationLease", hostId }),
  delete: (hostId, authorizationLease, threadId) =>
    requestHistorySnapshots({
      operation: "delete",
      hostId,
      authorizationLease,
      threadId,
    }),
  read: (hostId, authorizationLease, threadId) =>
    requestHistorySnapshots({
      operation: "read",
      hostId,
      authorizationLease,
      threadId,
    }),
  async subscribeAuthorizationLeaseInvalidation(hostId, callback) {
    const subscriptionId = crypto.randomUUID();
    historySnapshotInvalidationCallbacks.set(subscriptionId, callback);
    try {
      await requestHistorySnapshots({
        operation: "subscribeAuthorizationLeaseInvalidation",
        hostId,
        subscriptionId,
      });
    } catch (error) {
      historySnapshotInvalidationCallbacks.delete(subscriptionId);
      throw error;
    }

    let unsubscribed = false;
    return {
      unsubscribe(): void {
        if (unsubscribed) {
          return;
        }
        unsubscribed = true;
        historySnapshotInvalidationCallbacks.delete(subscriptionId);
        void requestHistorySnapshots({
          operation: "unsubscribeAuthorizationLeaseInvalidation",
          subscriptionId,
        }).catch((error) => {
          console.error(
            "[electron-stub] failed to unsubscribe history snapshot invalidation",
            error,
          );
        });
      },
    };
  },
  write: (hostId, authorizationLease, snapshotJson) =>
    requestHistorySnapshots({
      operation: "write",
      hostId,
      authorizationLease,
      snapshotJson,
    }),
};

const themeMediaQuery = matchMedia("(prefers-color-scheme: dark)");
const mobileMediaQuery = matchMedia("(max-width: 768px)");
const initialSidebarState = !mobileMediaQuery.matches;
const electronShim = (window.__ELECTRON_SHIM__ ??= {});
const APP_HOST_SERVICE_CHANNEL = "codex-web:app-host-service";

function appHostService(service: AppHostServiceName): AppHostService {
  return new Proxy(
    {},
    {
      get(_target, method): unknown {
        if (method === "then" || typeof method !== "string") {
          return undefined;
        }
        return (...args: unknown[]) =>
          invokeMain(APP_HOST_SERVICE_CHANNEL, [{ service, method, args }]);
      },
    },
  ) as AppHostService;
}

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
  projectWritableRoots: {
    ...electronShim.services?.projectWritableRoots,
    addRoot: async (request) => {
      if (request.root !== undefined) {
        return requestProjectWritableRoots("addRoot", request);
      }
      const root = await openSelectWorkspaceRootDialog({
        listDirectory: requestWorkspaceDirectoryEntries,
      });
      return requestProjectWritableRoots("addRoot", {
        ...request,
        ...(root === null ? {} : { root }),
      });
    },
    clearRoots: (request) => requestProjectWritableRoots("clearRoots", request),
    removeRoot: (request) => requestProjectWritableRoots("removeRoot", request),
  },
  threadProjectAssignments: {
    ...electronShim.services?.threadProjectAssignments,
    setAssignment: requestThreadProjectAssignment,
  },
  appServerHistorySnapshots,
  localEnvironments: appHostService("localEnvironments"),
  pluginScheduledTasks: appHostService("pluginScheduledTasks"),
  threadArchive: appHostService("threadArchive"),
  threadMetadataGeneration: appHostService("threadMetadataGeneration"),
  requestUserInputAutoResolution: {
    ...electronShim.services?.requestUserInputAutoResolution,
    recordConversationActivity: () => undefined,
    setConversationPresented: () => undefined,
    snooze: () => undefined,
  },
};

electronShim.overrideAdapter = {
  getDynamicConfigOverride(e) {
    if (e.name === "107580212") {
      return {
        ...e,
        value: {
          ...e.value,
          use_hidden_models: false,
        },
      };
    }

    return null;
  },
  getGateOverride(e) {
    if (e.name === "3902016271") {
      // guardian_approval: expose "Approve for me" in the web host.
      return {
        ...e,
        value: true,
      };
    }

    if (e.name === "3836321032") {
      // Enable the Pierre workspace file editor in the web host.
      return {
        ...e,
        value: true,
      };
    }

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
  getLayerOverride(e) {
    if (e.name !== "72216192") {
      return null;
    }

    return {
      ...e,
      __value: {
        ...e.__value,
        enable_i18n: true,
      },
    };
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
    sendRaw({ type: "bridge-disconnect" });
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
