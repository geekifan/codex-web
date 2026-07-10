import { WebSocket, type RawData } from "ws";
import {
  BRIDGE_HEADER_LENGTH,
  BRIDGE_PROTOCOL_VERSION,
  BridgeFrameType,
  decodeBridgeFrame,
  encodeBridgeFrame,
  encodeBridgeFramePayload,
  encodeBridgeValue,
  type BridgeFrameTypeValue,
} from "../shared/bridge-protocol";

export const RELIABLE_BRIDGE_PROTOCOL_VERSION = BRIDGE_PROTOCOL_VERSION;

const DEFAULT_GRACE_TIME_MS = 5 * 60 * 1_000;
const DEFAULT_MAX_UNACKED_BYTES = 64 * 1024 * 1_024;
const DEFAULT_MAX_IN_FLIGHT_BYTES = 256 * 1024;
const KEEPALIVE_INTERVAL_MS = 5_000;
const SOCKET_TIMEOUT_MS = 20_000;

export type ReliableBridgeHello = {
  type: "bridge-hello";
  protocolVersion: number;
  connectionId: string;
  serverEpoch: string | null;
};

type OutgoingMessage = {
  id: number;
  payload: Uint8Array;
  byteLength: number;
  resolve?: () => void;
  reject?: (error: Error) => void;
};

type ReliableBridgeSessionOptions<TIncoming> = {
  connectionId: string;
  serverEpoch: string;
  graceTimeMs?: number;
  maxUnackedBytes?: number;
  maxInFlightBytes?: number;
  onDispose: (reason: string) => void;
  onMessage: (message: TIncoming) => void;
};

export class ReliableBridgeSession<TIncoming, TOutgoing> {
  readonly connectionId: string;
  readonly serverEpoch: string;

  private readonly graceTimeMs: number;
  private readonly maxUnackedBytes: number;
  private readonly maxInFlightBytes: number;
  private readonly onDispose: (reason: string) => void;
  private readonly onMessage: (message: TIncoming) => void;
  private socket: WebSocket | null = null;
  private outgoingMessageId = 0;
  private outgoingAckId = 0;
  private outgoingSentId = 0;
  private outgoingUnackedBytes = 0;
  private outgoingUnacked: OutgoingMessage[] = [];
  private incomingMessageId = 0;
  private disposed = false;
  private graceTimer: NodeJS.Timeout | null = null;
  private keepaliveTimer: NodeJS.Timeout | null = null;
  private lastIncomingAt = Date.now();

  constructor(options: ReliableBridgeSessionOptions<TIncoming>) {
    this.connectionId = options.connectionId;
    this.serverEpoch = options.serverEpoch;
    this.graceTimeMs = options.graceTimeMs ?? DEFAULT_GRACE_TIME_MS;
    this.maxUnackedBytes = options.maxUnackedBytes ?? DEFAULT_MAX_UNACKED_BYTES;
    this.maxInFlightBytes =
      options.maxInFlightBytes ?? DEFAULT_MAX_IN_FLIGHT_BYTES;
    this.onDispose = options.onDispose;
    this.onMessage = options.onMessage;
  }

  attach(socket: WebSocket): void {
    if (this.disposed) {
      socket.close(1012, "bridge session disposed");
      return;
    }

    this.clearGraceTimer();
    this.replaceSocket(socket);
    this.lastIncomingAt = Date.now();

    socket.on("message", this.handleSocketMessage);
    socket.on("close", this.handleSocketClose);
    socket.on("error", this.handleSocketError);

    this.writeControl({
      type: "bridge-ready",
      connectionId: this.connectionId,
      serverEpoch: this.serverEpoch,
    });
    this.writeAck();
    this.outgoingSentId = this.outgoingAckId;
    this.pumpOutgoing();
    this.startKeepalive();
  }

  send(message: TOutgoing): void {
    this.enqueue(message);
  }

  sendAndWait(message: TOutgoing): Promise<void> {
    if (this.disposed) {
      return Promise.reject(new Error("reliable bridge session disposed"));
    }
    return new Promise<void>((resolve, reject) => {
      this.enqueue(message, resolve, reject);
    });
  }

  dispose(reason: string): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.clearGraceTimer();
    this.stopKeepalive();
    const socket = this.socket;
    this.socket = null;
    this.removeSocketListeners(socket);
    socket?.close(1000, reason);
    const error = new Error(reason);
    for (const message of this.outgoingUnacked) {
      message.reject?.(error);
    }
    this.outgoingUnacked = [];
    this.outgoingUnackedBytes = 0;
    this.onDispose(reason);
  }

  private enqueue(
    message: TOutgoing,
    resolve?: () => void,
    reject?: (error: Error) => void,
  ): void {
    if (this.disposed) {
      reject?.(new Error("reliable bridge session disposed"));
      return;
    }
    let payload: Uint8Array;
    try {
      payload = encodeBridgeValue(message);
    } catch (error) {
      reject?.(error instanceof Error ? error : new Error(String(error)));
      if (!reject) {
        this.reset("failed to encode reliable bridge message");
      }
      return;
    }
    const outgoing = {
      id: ++this.outgoingMessageId,
      payload,
      byteLength: BRIDGE_HEADER_LENGTH + payload.byteLength,
      ...(resolve ? { resolve } : {}),
      ...(reject ? { reject } : {}),
    };
    this.outgoingUnacked.push(outgoing);
    this.outgoingUnackedBytes += outgoing.byteLength;

    if (this.outgoingUnackedBytes > this.maxUnackedBytes) {
      this.reset("reliable bridge buffer exceeded");
      return;
    }
    this.pumpOutgoing();
  }

  private readonly handleSocketMessage = (
    rawData: RawData,
    isBinary: boolean,
  ): void => {
    this.lastIncomingAt = Date.now();
    if (!isBinary) {
      this.reset("reliable bridge requires binary frames");
      return;
    }
    let frame;
    try {
      frame = decodeBridgeFrame(toUint8Array(rawData));
    } catch {
      this.reset("invalid reliable bridge frame");
      return;
    }

    if (frame.type === BridgeFrameType.Regular) {
      if (!this.acceptAck(frame.ack)) {
        return;
      }
      this.acceptMessage(frame.id, frame.payload);
      return;
    }
    if (frame.type === BridgeFrameType.Ack) {
      this.acceptAck(frame.ack);
      return;
    }
    if (frame.type === BridgeFrameType.ReplayRequest) {
      if (this.acceptAck(frame.ack, false)) {
        this.outgoingSentId = frame.ack;
        this.pumpOutgoing();
      }
      return;
    }
    if (frame.type === BridgeFrameType.Disconnect) {
      this.dispose("client disconnected");
      return;
    }
    if (frame.type !== BridgeFrameType.KeepAlive) {
      this.reset("unsupported reliable bridge frame");
    }
  };

  private readonly handleSocketClose = (): void => {
    const socket = this.socket;
    if (!socket) {
      return;
    }
    this.socket = null;
    this.removeSocketListeners(socket);
    this.stopKeepalive();
    this.graceTimer = setTimeout(() => {
      this.dispose("reconnection grace period expired");
    }, this.graceTimeMs);
    this.graceTimer.unref?.();
  };

  private readonly handleSocketError = (): void => {
    this.socket?.terminate();
  };

  private acceptMessage(id: number, message: unknown): void {
    if (!Number.isSafeInteger(id) || id <= 0 || message === undefined) {
      this.reset("invalid reliable bridge message");
      return;
    }
    if (id === this.incomingMessageId + 1) {
      this.incomingMessageId = id;
      try {
        this.onMessage(message as TIncoming);
      } catch {
        this.reset("reliable bridge message handler failed");
        return;
      }
      this.writeAck();
      return;
    }
    if (id <= this.incomingMessageId) {
      this.writeAck();
      return;
    }
    this.writeFrame(BridgeFrameType.ReplayRequest, 0, this.incomingMessageId);
  }

  private acceptAck(ack: number, pump = true): boolean {
    if (!Number.isSafeInteger(ack) || ack < 0 || ack > this.outgoingMessageId) {
      this.reset("invalid reliable bridge acknowledgement");
      return false;
    }
    if (ack <= this.outgoingAckId) {
      return true;
    }

    this.outgoingAckId = ack;
    const acknowledged = this.outgoingUnacked.filter(
      (message) => message.id <= ack,
    );
    this.outgoingUnackedBytes -= acknowledged.reduce(
      (total, message) => total + message.byteLength,
      0,
    );
    this.outgoingUnacked = this.outgoingUnacked.filter(
      (message) => message.id > ack,
    );
    for (const message of acknowledged) {
      message.resolve?.();
    }
    if (pump) {
      this.pumpOutgoing();
    }
    return true;
  }

  private pumpOutgoing(): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return;
    }

    let inFlightBytes = this.outgoingUnacked
      .filter((message) => message.id <= this.outgoingSentId)
      .reduce((total, message) => total + message.byteLength, 0);
    for (const message of this.outgoingUnacked) {
      if (message.id <= this.outgoingSentId) {
        continue;
      }
      if (
        inFlightBytes > 0 &&
        inFlightBytes + message.byteLength > this.maxInFlightBytes
      ) {
        break;
      }
      this.writeEncodedData(message);
      this.outgoingSentId = message.id;
      inFlightBytes += message.byteLength;
    }
  }

  private writeEncodedData(message: OutgoingMessage): void {
    this.writeFrame(
      BridgeFrameType.Regular,
      message.id,
      this.incomingMessageId,
      message.payload,
      true,
    );
  }

  private writeControl(payload: unknown): void {
    this.writeFrame(
      BridgeFrameType.Control,
      0,
      this.incomingMessageId,
      payload,
    );
  }

  private writeAck(): void {
    this.writeFrame(BridgeFrameType.Ack, 0, this.incomingMessageId);
  }

  private writeFrame(
    type: BridgeFrameTypeValue,
    id: number,
    ack: number,
    payload?: unknown,
    encoded = false,
  ): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return;
    }
    try {
      const frame = encoded
        ? encodeBridgeFramePayload(type, id, ack, payload as Uint8Array)
        : encodeBridgeFrame({ type, id, ack, payload });
      this.socket.send(frame, { binary: true });
    } catch {
      this.socket.terminate();
    }
  }

  private reset(reason: string): void {
    this.writeControl({ type: "bridge-reset", reason });
    this.dispose(reason);
  }

  private replaceSocket(socket: WebSocket): void {
    const previousSocket = this.socket;
    this.socket = socket;
    if (!previousSocket || previousSocket === socket) {
      return;
    }
    this.removeSocketListeners(previousSocket);
    previousSocket.terminate();
  }

  private removeSocketListeners(socket: WebSocket | null): void {
    socket?.off("message", this.handleSocketMessage);
    socket?.off("close", this.handleSocketClose);
    socket?.off("error", this.handleSocketError);
  }

  private startKeepalive(): void {
    this.stopKeepalive();
    this.keepaliveTimer = setInterval(() => {
      if (Date.now() - this.lastIncomingAt > SOCKET_TIMEOUT_MS) {
        this.socket?.terminate();
        return;
      }
      this.writeFrame(BridgeFrameType.KeepAlive, 0, this.incomingMessageId);
    }, KEEPALIVE_INTERVAL_MS);
    this.keepaliveTimer.unref?.();
  }

  private stopKeepalive(): void {
    if (this.keepaliveTimer) {
      clearInterval(this.keepaliveTimer);
      this.keepaliveTimer = null;
    }
  }

  private clearGraceTimer(): void {
    if (this.graceTimer) {
      clearTimeout(this.graceTimer);
      this.graceTimer = null;
    }
  }
}

export function parseReliableBridgeHello(
  value: unknown,
): ReliableBridgeHello | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const hello = value as Partial<ReliableBridgeHello>;
  if (
    hello.type !== "bridge-hello" ||
    hello.protocolVersion !== RELIABLE_BRIDGE_PROTOCOL_VERSION ||
    typeof hello.connectionId !== "string" ||
    hello.connectionId.length === 0 ||
    (hello.serverEpoch !== null && typeof hello.serverEpoch !== "string")
  ) {
    return null;
  }
  return hello as ReliableBridgeHello;
}

function toUint8Array(rawData: RawData): Uint8Array {
  if (Array.isArray(rawData)) {
    const length = rawData.reduce(
      (total, chunk) => total + chunk.byteLength,
      0,
    );
    const result = new Uint8Array(length);
    let offset = 0;
    for (const chunk of rawData) {
      result.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return result;
  }
  if (rawData instanceof ArrayBuffer) {
    return new Uint8Array(rawData);
  }
  return new Uint8Array(rawData.buffer, rawData.byteOffset, rawData.byteLength);
}
