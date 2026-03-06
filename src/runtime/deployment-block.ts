import { MULTIVERSX_API_URL } from "../constants";

/**
 * Fetches the deployment block (round) of a contract from the MultiversX API.
 * Uses the first transaction where the account is involved (order=asc) as a proxy for deployment.
 */
export async function getDeploymentBlock(
  contractAddress: string,
  apiBaseUrl: string = MULTIVERSX_API_URL,
): Promise<number | null> {
  const url = `${apiBaseUrl.replace(/\/$/, "")}/accounts/${encodeURIComponent(contractAddress)}/transactions?size=1&order=asc`;
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = (await res.json()) as Array<{ round?: number }>;
    if (!Array.isArray(data) || data.length === 0) return null;
    const round = data[0]?.round;
    return typeof round === "number" ? round : null;
  } catch {
    return null;
  }
}

/**
 * Fetches the latest block round from the MultiversX API.
 */
export async function getLatestBlock(
  apiBaseUrl: string = MULTIVERSX_API_URL,
): Promise<number | null> {
  const url = `${apiBaseUrl.replace(/\/$/, "")}/blocks?size=1&order=desc`;
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = (await res.json()) as Array<{ round?: number }>;
    if (!Array.isArray(data) || data.length === 0) return null;
    const round = data[0]?.round;
    return typeof round === "number" ? round : null;
  } catch {
    return null;
  }
}
