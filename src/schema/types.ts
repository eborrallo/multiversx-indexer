export interface MultiversXEvent {
  id: string;
  sourceId: string;
  txHash: string;
  timestamp: number;
  address: string;
  identifier: string;
  topics: string[];
  data: string | null;
  additionalData: string[];
  eventIndex: number;
}

export function makeEventId(sourceId: string, txHash: string, eventIndex: number): string {
  return `${sourceId}:${txHash}:${eventIndex}`;
}

export function base64ToBigInt(b64: string): bigint {
  if (!b64) return 0n;
  const bytes = Buffer.from(b64, "base64");
  if (bytes.length === 0) return 0n;
  let value = 0n;
  for (const byte of bytes) {
    value = (value << 8n) | BigInt(byte & 0xff);
  }
  return value;
}

export function base64ToUtf8(b64: string): string {
  if (!b64) return "";
  return Buffer.from(b64, "base64").toString("utf8");
}

export function utf8ToBase64(str: string): string {
  return Buffer.from(str, "utf8").toString("base64");
}

/** UTF-8 string to hex (for events index topics). */
export function utf8ToHex(str: string): string {
  return Buffer.from(str, "utf8").toString("hex");
}

/** Hex to UTF-8 string. */
export function hexToUtf8(hex: string): string {
  if (!hex) return "";
  try {
    return Buffer.from(hex, "hex").toString("utf8");
  } catch {
    return "";
  }
}
