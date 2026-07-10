export const BRIDGE_PROTOCOL_VERSION = 3;
export const BRIDGE_HEADER_LENGTH = 13;
export const MAX_BRIDGE_PAYLOAD_BYTES = 64 * 1024 * 1024;

export const BridgeFrameType = {
  Regular: 1,
  Control: 2,
  Ack: 3,
  Disconnect: 5,
  ReplayRequest: 6,
  KeepAlive: 9,
} as const;

export type BridgeFrameTypeValue =
  (typeof BridgeFrameType)[keyof typeof BridgeFrameType];

export type BridgeFrame = {
  type: BridgeFrameTypeValue;
  id: number;
  ack: number;
  payload?: unknown;
};

const ValueType = {
  Undefined: 0,
  Null: 1,
  False: 2,
  True: 3,
  Number: 4,
  String: 5,
  Bytes: 6,
  Array: 7,
  Object: 8,
} as const;

const MAX_VALUE_DEPTH = 100;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

class ByteWriter {
  private readonly chunks: Uint8Array[] = [];
  private length = 0;

  writeUInt8(value: number): void {
    const buffer = new Uint8Array(1);
    buffer[0] = value;
    this.push(buffer);
  }

  writeUInt32(value: number): void {
    const buffer = new Uint8Array(4);
    new DataView(buffer.buffer).setUint32(0, value, false);
    this.push(buffer);
  }

  writeFloat64(value: number): void {
    const buffer = new Uint8Array(8);
    new DataView(buffer.buffer).setFloat64(0, value, false);
    this.push(buffer);
  }

  writeLengthPrefixed(value: Uint8Array): void {
    this.writeUInt32(value.byteLength);
    this.push(value);
  }

  toUint8Array(): Uint8Array {
    const result = new Uint8Array(this.length);
    let offset = 0;
    for (const chunk of this.chunks) {
      result.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return result;
  }

  private push(value: Uint8Array): void {
    this.length += value.byteLength;
    if (this.length > MAX_BRIDGE_PAYLOAD_BYTES) {
      throw new Error("bridge payload exceeds maximum size");
    }
    this.chunks.push(value);
  }
}

class ByteReader {
  private offset = 0;

  constructor(private readonly buffer: Uint8Array) {}

  get remaining(): number {
    return this.buffer.byteLength - this.offset;
  }

  readUInt8(): number {
    this.ensureAvailable(1);
    return this.buffer[this.offset++]!;
  }

  readUInt32(): number {
    this.ensureAvailable(4);
    const value = new DataView(
      this.buffer.buffer,
      this.buffer.byteOffset + this.offset,
      4,
    ).getUint32(0, false);
    this.offset += 4;
    return value;
  }

  readFloat64(): number {
    this.ensureAvailable(8);
    const value = new DataView(
      this.buffer.buffer,
      this.buffer.byteOffset + this.offset,
      8,
    ).getFloat64(0, false);
    this.offset += 8;
    return value;
  }

  readLengthPrefixed(): Uint8Array {
    return this.readBytes(this.readUInt32());
  }

  readBytes(length: number): Uint8Array {
    this.ensureAvailable(length);
    const value = this.buffer.subarray(this.offset, this.offset + length);
    this.offset += length;
    return value;
  }

  private ensureAvailable(length: number): void {
    if (
      !Number.isSafeInteger(length) ||
      length < 0 ||
      length > this.remaining
    ) {
      throw new Error("invalid bridge payload length");
    }
  }
}

export function encodeBridgeValue(value: unknown): Uint8Array {
  const writer = new ByteWriter();
  writeValue(writer, value, 0);
  return writer.toUint8Array();
}

export function decodeBridgeValue(buffer: Uint8Array): unknown {
  if (buffer.byteLength > MAX_BRIDGE_PAYLOAD_BYTES) {
    throw new Error("bridge payload exceeds maximum size");
  }
  const reader = new ByteReader(buffer);
  const value = readValue(reader, 0);
  if (reader.remaining !== 0) {
    throw new Error("unexpected trailing bridge payload bytes");
  }
  return value;
}

function writeValue(writer: ByteWriter, value: unknown, depth: number): void {
  if (depth > MAX_VALUE_DEPTH) {
    throw new Error("bridge value exceeds maximum depth");
  }
  if (value === undefined) {
    writer.writeUInt8(ValueType.Undefined);
    return;
  }
  if (value === null) {
    writer.writeUInt8(ValueType.Null);
    return;
  }
  if (value === false) {
    writer.writeUInt8(ValueType.False);
    return;
  }
  if (value === true) {
    writer.writeUInt8(ValueType.True);
    return;
  }
  if (typeof value === "number") {
    writer.writeUInt8(ValueType.Number);
    writer.writeFloat64(value);
    return;
  }
  if (typeof value === "string") {
    writer.writeUInt8(ValueType.String);
    writer.writeLengthPrefixed(textEncoder.encode(value));
    return;
  }
  if (value instanceof Uint8Array) {
    writer.writeUInt8(ValueType.Bytes);
    writer.writeLengthPrefixed(value);
    return;
  }
  if (Array.isArray(value)) {
    writer.writeUInt8(ValueType.Array);
    writer.writeUInt32(value.length);
    for (const item of value) {
      writeValue(writer, item, depth + 1);
    }
    return;
  }
  if (typeof value === "object") {
    const entries = Object.entries(value);
    writer.writeUInt8(ValueType.Object);
    writer.writeUInt32(entries.length);
    for (const [key, item] of entries) {
      writer.writeLengthPrefixed(textEncoder.encode(key));
      writeValue(writer, item, depth + 1);
    }
    return;
  }
  throw new Error(`unsupported bridge value type: ${typeof value}`);
}

function readValue(reader: ByteReader, depth: number): unknown {
  if (depth > MAX_VALUE_DEPTH) {
    throw new Error("bridge value exceeds maximum depth");
  }
  switch (reader.readUInt8()) {
    case ValueType.Undefined:
      return undefined;
    case ValueType.Null:
      return null;
    case ValueType.False:
      return false;
    case ValueType.True:
      return true;
    case ValueType.Number:
      return reader.readFloat64();
    case ValueType.String:
      return textDecoder.decode(reader.readLengthPrefixed());
    case ValueType.Bytes:
      return reader.readLengthPrefixed().slice();
    case ValueType.Array: {
      const length = reader.readUInt32();
      const value: unknown[] = [];
      for (let index = 0; index < length; index += 1) {
        value.push(readValue(reader, depth + 1));
      }
      return value;
    }
    case ValueType.Object: {
      const length = reader.readUInt32();
      const value: Record<string, unknown> = {};
      for (let index = 0; index < length; index += 1) {
        const key = textDecoder.decode(reader.readLengthPrefixed());
        if (Object.hasOwn(value, key)) {
          throw new Error("duplicate bridge object key");
        }
        Object.defineProperty(value, key, {
          configurable: true,
          enumerable: true,
          value: readValue(reader, depth + 1),
          writable: true,
        });
      }
      return value;
    }
    default:
      throw new Error("unsupported bridge value tag");
  }
}

export function encodeBridgeFrame(frame: BridgeFrame): Uint8Array {
  validateFrameNumber(frame.id, "id");
  validateFrameNumber(frame.ack, "ack");
  const payload =
    frame.payload === undefined
      ? new Uint8Array()
      : encodeBridgeValue(frame.payload);
  return encodeBridgeFramePayload(frame.type, frame.id, frame.ack, payload);
}

export function encodeBridgeFramePayload(
  type: BridgeFrameTypeValue,
  id: number,
  ack: number,
  payload: Uint8Array,
): Uint8Array {
  validateFrameNumber(id, "id");
  validateFrameNumber(ack, "ack");
  if (payload.byteLength > MAX_BRIDGE_PAYLOAD_BYTES) {
    throw new Error("bridge payload exceeds maximum size");
  }
  const buffer = new Uint8Array(BRIDGE_HEADER_LENGTH + payload.byteLength);
  const view = new DataView(buffer.buffer);
  view.setUint8(0, type);
  view.setUint32(1, id, false);
  view.setUint32(5, ack, false);
  view.setUint32(9, payload.byteLength, false);
  buffer.set(payload, BRIDGE_HEADER_LENGTH);
  return buffer;
}

export function decodeBridgeFrame(buffer: Uint8Array): BridgeFrame {
  if (buffer.byteLength < BRIDGE_HEADER_LENGTH) {
    throw new Error("invalid bridge frame header");
  }
  const view = new DataView(
    buffer.buffer,
    buffer.byteOffset,
    buffer.byteLength,
  );
  const type = view.getUint8(0);
  if (!Object.values(BridgeFrameType).includes(type as BridgeFrameTypeValue)) {
    throw new Error("unsupported bridge frame type");
  }
  const payloadLength = view.getUint32(9, false);
  if (
    payloadLength > MAX_BRIDGE_PAYLOAD_BYTES ||
    payloadLength !== buffer.byteLength - BRIDGE_HEADER_LENGTH
  ) {
    throw new Error("invalid bridge frame payload length");
  }
  const payloadBytes = buffer.subarray(BRIDGE_HEADER_LENGTH);
  return {
    type: type as BridgeFrameTypeValue,
    id: view.getUint32(1, false),
    ack: view.getUint32(5, false),
    ...(payloadBytes.byteLength === 0
      ? {}
      : { payload: decodeBridgeValue(payloadBytes) }),
  };
}

function validateFrameNumber(value: number, label: string): void {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new Error(`invalid bridge frame ${label}`);
  }
}
