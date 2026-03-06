import { afterEach, beforeEach, describe, expect, mock, spyOn, test } from "bun:test";
import type {
  AccountOnChain,
  ChainReader,
  ContractQueryResult,
  TransactionOnChain,
} from "./chain-client";
import {
  createCachedChainClient,
  createChainClient,
  createChainClientsForConfig,
} from "./chain-client";
import { bootstrapInternalSchema, createDatabase } from "./db";

const MEMORY_DATA_DIR = "memory://";

/** Run with: INTEGRATION=1 bun test src/runtime/chain-client.test.ts */
const RUN_INTEGRATION = Boolean(process.env.INTEGRATION);

describe("createChainClient (integration - real API)", () => {
  test.skipIf(!RUN_INTEGRATION)(
    "getTransaction reads real tx from mainnet",
    async () => {
      const client = createChainClient("https://api.multiversx.com");
      const txHash = "5401ee4f6e9b48a5704e98929c8e425d5b208f76615cfb18c9122a2a42dc22e7";

      const result = await client.getTransaction(txHash);

      expect(result).not.toBeNull();
      expect(result?.txHash).toBe(txHash);
      expect(result?.sender).toBe("erd1rvj0t0sw5fwukdv869wtggnwferxm76rq6yr6tfw0ug5fzg5ksnqayypda");
      expect(result?.receiver).toBe(
        "erd1qqqqqqqqqqqqqpgqmksd4gl3xau5eja42sp6qmrxewxgj0ny4d3qfksmrq",
      );
      expect(result?.status).toBe("success");
      // MultiversX API returns "round" as block number
      expect(result?.round).toBe(29_449_466);
    },
    15000,
  );

  test.skipIf(!RUN_INTEGRATION)(
    "getAccount reads real account from mainnet",
    async () => {
      const client = createChainClient("https://api.multiversx.com");
      const address = "erd1rvj0t0sw5fwukdv869wtggnwferxm76rq6yr6tfw0ug5fzg5ksnqayypda";

      const result = await client.getAccount(address);

      expect(result).not.toBeNull();
      expect(result?.address).toBe(address);
      expect(typeof result?.balance).toBe("string");
      expect(typeof result?.nonce).toBe("number");
    },
    15000,
  );
});

describe("createChainClient", () => {
  let fetchSpy: ReturnType<typeof spyOn>;

  beforeEach(() => {
    fetchSpy = spyOn(globalThis, "fetch");
  });

  afterEach(() => {
    fetchSpy.mockRestore();
  });

  test("getAccount returns account on 200", async () => {
    const account: AccountOnChain = {
      address: "erd1abc",
      balance: "1000000000000000000",
      nonce: 5,
    };
    fetchSpy.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(account),
    } as Response);

    const client = createChainClient("https://api.test.com");
    const result = await client.getAccount("erd1abc");

    expect(result).toEqual(account);
    expect(fetchSpy).toHaveBeenCalledWith("https://api.test.com/accounts/erd1abc");
  });

  test("getAccount returns null on 404", async () => {
    fetchSpy.mockResolvedValue({ ok: false, status: 404 } as Response);

    const client = createChainClient("https://api.test.com");
    const result = await client.getAccount("erd1nonexistent");

    expect(result).toBeNull();
  });

  test("getAccount returns null on fetch error", async () => {
    fetchSpy.mockRejectedValue(new Error("network error"));

    const client = createChainClient("https://api.test.com");
    const result = await client.getAccount("erd1abc");

    expect(result).toBeNull();
  });

  test("getAccount encodes address in URL", async () => {
    fetchSpy.mockResolvedValue({ ok: true, json: () => Promise.resolve({}) } as Response);

    const client = createChainClient("https://api.test.com");
    await client.getAccount("erd1q@special#chars");

    expect(fetchSpy).toHaveBeenCalledWith(expect.stringContaining("/accounts/"));
  });

  test("getTransaction returns transaction on 200", async () => {
    const tx: TransactionOnChain = {
      txHash: "abc123",
      sender: "erd1a",
      receiver: "erd1b",
      value: "1000",
    };
    fetchSpy.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(tx),
    } as Response);

    const client = createChainClient("https://api.test.com");
    const result = await client.getTransaction("abc123");

    expect(result).toEqual(tx);
    expect(fetchSpy).toHaveBeenCalledWith("https://api.test.com/transactions/abc123");
  });

  test("getTransaction returns null on 404", async () => {
    fetchSpy.mockResolvedValue({ ok: false, status: 404 } as Response);

    const client = createChainClient("https://api.test.com");
    const result = await client.getTransaction("nonexistent");

    expect(result).toBeNull();
  });

  test("queryContract returns result on 200", async () => {
    const queryResult: ContractQueryResult = {
      returnData: ["dGVzdA=="],
      returnCode: "ok",
    };
    fetchSpy.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve(queryResult),
    } as Response);

    const client = createChainClient("https://api.test.com");
    const result = await client.queryContract("erd1contract", "getRewards", ["arg1"]);

    expect(result).toEqual(queryResult);
    expect(fetchSpy).toHaveBeenCalledWith(
      "https://api.test.com/vm-values/query",
      expect.objectContaining({
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          scAddress: "erd1contract",
          funcName: "getRewards",
          args: ["arg1"],
        }),
      }),
    );
  });

  test("queryContract strips 0x prefix from args", async () => {
    fetchSpy.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ returnData: [], returnCode: "ok" }),
    } as Response);

    const client = createChainClient("https://api.test.com");
    await client.queryContract("erd1c", "get", ["0x1234", "5678"]);

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        body: JSON.stringify({
          scAddress: "erd1c",
          funcName: "get",
          args: ["1234", "5678"],
        }),
      }),
    );
  });

  test("queryContract throws on non-ok response", async () => {
    fetchSpy.mockResolvedValue({
      ok: false,
      status: 500,
      text: () => Promise.resolve("Internal Server Error"),
    } as Response);

    const client = createChainClient("https://api.test.com");

    await expect(client.queryContract("erd1c", "get")).rejects.toThrow(/VM query failed 500/);
  });

  test("normalizes API URL (removes trailing slash)", async () => {
    fetchSpy.mockResolvedValue({ ok: true, json: () => Promise.resolve({}) } as Response);

    const client = createChainClient("https://api.test.com/");
    await client.getAccount("erd1a");

    expect(fetchSpy).toHaveBeenCalledWith("https://api.test.com/accounts/erd1a");
  });
});

describe("createCachedChainClient", () => {
  let db: Awaited<ReturnType<typeof createDatabase>>["db"];
  let close: () => Promise<void>;

  beforeEach(async () => {
    const conn = await createDatabase({ dataDir: MEMORY_DATA_DIR });
    db = conn.db;
    close = conn.close;
    await bootstrapInternalSchema(db);
  });

  afterEach(async () => {
    await close();
  });

  test("getAccount caches result and returns on second call", async () => {
    const account: AccountOnChain = { address: "erd1a", balance: "100", nonce: 0 };
    const inner: ChainReader = {
      getAccount: mock(() => Promise.resolve(account)),
      getTransaction: mock(() => Promise.resolve(null)),
      queryContract: mock(() => Promise.resolve({ returnData: [], returnCode: "ok" })),
    };

    const client = createCachedChainClient(inner, db, "src1");

    const r1 = await client.getAccount("erd1a");
    const r2 = await client.getAccount("erd1a");

    expect(r1).toEqual(account);
    expect(r2).toEqual(account);
    expect(inner.getAccount).toHaveBeenCalledTimes(1);
  });

  test("getTransaction caches result", async () => {
    const tx: TransactionOnChain = {
      txHash: "tx1",
      sender: "erd1a",
      receiver: "erd1b",
      value: "100",
    };
    const inner: ChainReader = {
      getAccount: mock(() => Promise.resolve(null)),
      getTransaction: mock(() => Promise.resolve(tx)),
      queryContract: mock(() => Promise.resolve({ returnData: [], returnCode: "ok" })),
    };

    const client = createCachedChainClient(inner, db, "src1");

    const r1 = await client.getTransaction("tx1");
    const r2 = await client.getTransaction("tx1");

    expect(r1).toEqual(tx);
    expect(r2).toEqual(tx);
    expect(inner.getTransaction).toHaveBeenCalledTimes(1);
  });

  test("queryContract caches result", async () => {
    const result: ContractQueryResult = { returnData: ["data"], returnCode: "ok" };
    const inner: ChainReader = {
      getAccount: mock(() => Promise.resolve(null)),
      getTransaction: mock(() => Promise.resolve(null)),
      queryContract: mock(() => Promise.resolve(result)),
    };

    const client = createCachedChainClient(inner, db, "src1");

    const r1 = await client.queryContract("erd1c", "get", ["a"]);
    const r2 = await client.queryContract("erd1c", "get", ["a"]);

    expect(r1).toEqual(result);
    expect(r2).toEqual(result);
    expect(inner.queryContract).toHaveBeenCalledTimes(1);
  });

  test("cache is isolated by sourceId", async () => {
    const account: AccountOnChain = { address: "erd1a", balance: "100", nonce: 0 };
    const inner: ChainReader = {
      getAccount: mock(() => Promise.resolve(account)),
      getTransaction: mock(() => Promise.resolve(null)),
      queryContract: mock(() => Promise.resolve({ returnData: [], returnCode: "ok" })),
    };

    const client1 = createCachedChainClient(inner, db, "src1");
    const client2 = createCachedChainClient(inner, db, "src2");

    await client1.getAccount("erd1a");
    await client2.getAccount("erd1a");

    expect(inner.getAccount).toHaveBeenCalledTimes(2);
  });

  test("caches null for getAccount", async () => {
    const inner: ChainReader = {
      getAccount: mock(() => Promise.resolve(null)),
      getTransaction: mock(() => Promise.resolve(null)),
      queryContract: mock(() => Promise.resolve({ returnData: [], returnCode: "ok" })),
    };

    const client = createCachedChainClient(inner, db, "src1");

    const r1 = await client.getAccount("erd1nonexistent");
    const r2 = await client.getAccount("erd1nonexistent");

    expect(r1).toBeNull();
    expect(r2).toBeNull();
    expect(inner.getAccount).toHaveBeenCalledTimes(1);
  });
});

describe("createChainClientsForConfig", () => {
  let db: Awaited<ReturnType<typeof createDatabase>>["db"];
  let close: () => Promise<void>;

  beforeEach(async () => {
    const conn = await createDatabase({ dataDir: MEMORY_DATA_DIR });
    db = conn.db;
    close = conn.close;
    await bootstrapInternalSchema(db);
  });

  afterEach(async () => {
    await close();
  });

  test("creates clients for kepler sources", () => {
    const map = createChainClientsForConfig(
      [
        { id: "mainnet", type: "kepler" },
        { id: "devnet", type: "kepler", multiversxApiUrl: "https://devnet-api.multiversx.com" },
      ],
      db,
    );

    expect(map.size).toBe(2);
    expect(map.has("mainnet")).toBe(true);
    expect(map.has("devnet")).toBe(true);
  });

  test("skips non-kepler sources", () => {
    const map = createChainClientsForConfig(
      [
        { id: "kepler1", type: "kepler" },
        { id: "other", type: "other" },
      ],
      db,
    );

    expect(map.size).toBe(1);
    expect(map.has("kepler1")).toBe(true);
    expect(map.has("other")).toBe(false);
  });

  test("returns empty map when no kepler sources", () => {
    const map = createChainClientsForConfig([{ id: "x", type: "other" }], db);

    expect(map.size).toBe(0);
  });

  test("clients expose getAccount, getTransaction, queryContract", () => {
    const map = createChainClientsForConfig([{ id: "mainnet", type: "kepler" }], db);

    const client = map.get("mainnet");
    expect(client).toBeDefined();
    expect(typeof client?.getAccount).toBe("function");
    expect(typeof client?.getTransaction).toBe("function");
    expect(typeof client?.queryContract).toBe("function");
  });
});
