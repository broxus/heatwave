import process from "node:process";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";

import {
  Address,
  ProviderRpcClient,
  TransactionId,
} from "everscale-inpage-provider";
import {
  EverscaleStandaloneClient,
  SimpleKeystore,
  Clock,
  SimpleAccountsStorage,
  Account,
  WalletV3Account,
  HighloadWalletV2,
  EverWalletAccount,
  GiverAccount,
} from "everscale-standalone-client/nodejs";
import * as nt from "nekoton-wasm";

const STATES_API_URL: string = "https://states.everscan.io";
const CACHE_DIR: string = "heatwave-states";
const TIMESTAMP_BIAS: number = 3600;

type FrozenAccount = {
  address: Address;
  state: string;
  storageFeeDebt: string;
};

const clock = new Clock();

function printHelp() {
  console.log(`\
Usage: npm run unfreeze <path> --giver=<wallet> --sign=<sign> [--target-balance=<target-balance>] [--ignore-cache]

A tool for unfreezing Everscale accounts.

Positional Arguments:
  param             path to the CSV file with accounts list

Options:
  --giver           giver contract address
  --sign            path to keys.json
  --target-balance  target balance in nano EVERs; default: 1000000000
  --ignore-cache    ignore computed states cache
  --help            display usage information
`);
}

async function app(args: Args) {
  const parseArgs = () => {
    const parseSwitch = (name: string): boolean => {
      const value = args.flags[name];
      if (value === true) {
        return value;
      } else if (value != null && value !== "true" && value !== "false") {
        throw new Error(`Expected a boolean value for \`${name}\``);
      } else {
        return value === "true";
      }
    };

    const res = {
      path: args.positional[0],
      giver: args.flags["giver"] as string,
      sign: args.flags["sign"] as string,
      ignoreCache: parseSwitch("ignore-cache"),
      targetBalance:
        (args.flags["target-balance"] as string | undefined) || "1000000000",
    };

    const separator = "\n  ";
    const missing = [
      res.path == null ? "path" : "",
      typeof res.giver != "string" ? "--giver" : "",
      typeof res.sign != "string" ? "--sign" : "",
    ]
      .filter((item) => item.length > 0)
      .join(separator);

    if (missing.length > 0) {
      throw new Error(`Required options not provided:${separator}${missing}`);
    }

    if (!checkAddress(res.giver)) {
      throw new Error("Invalid giver address");
    }

    if (res.targetBalance != null) {
      if (typeof res.targetBalance == "string") {
        parseInt(res.targetBalance);
      } else {
        throw new Error("Expected a number value for `target-balance`");
      }
    }

    return res;
  };

  const parsedArgs = parseArgs();

  const keystore = new SimpleKeystore();
  const publicKey = await fs
    .readFile(path.resolve(__dirname, parsedArgs.sign), "utf8")
    .then((text) => {
      const data: any = JSON.parse(text);
      if (
        typeof data != "object" ||
        typeof data.public != "string" ||
        typeof data.secret != "string"
      ) {
        throw new Error("Invalid keys");
      }

      keystore.addKeyPair({
        publicKey: data.public as string,
        secretKey: data.secret as string,
      });
      return data.public as string;
    });

  const accountsStorage = new SimpleAccountsStorage();

  const provider = new ProviderRpcClient({
    forceUseFallback: true,
    fallback: () =>
      EverscaleStandaloneClient.create({
        clock,
        connection: "mainnetJrpc",
        keystore,
        accountsStorage,
      }),
  });
  await provider.ensureInitialized();

  const giverAddress = new Address(parsedArgs.giver);
  await guessGiverAccount(provider, giverAddress, publicKey).then((account) =>
    accountsStorage.addAccount(account)
  );

  // Parse address list
  const accounts = await readAccounts(parsedArgs.path);
  console.log(`Input: ${accounts.length} accounts`);

  // Compute system addresses
  const computeAddr = (boc: string) =>
    provider.getBocHash(boc).then((hash) => new Address(`0:${hash}`));
  const microwaveAddress = await computeAddr(MICROWAVE_BOC);
  console.log(`Microwave address: ${microwaveAddress}`);

  // Compute states
  console.log("Preparing states");
  const states = await prepareStates(
    provider,
    accounts,
    parsedArgs.ignoreCache
  );

  // Unfreeze accounts
  console.log("Unfreezing contracts");

  const subscriber = new provider.Subscriber();

  const microwave = new provider.Contract(MICROWAVE_ABI, microwaveAddress);

  const TX_FEE = BigInt(100000000); // 0.1 EVER
  for (const item of states) {
    const amount =
      BigInt(item.storageFeeDebt) + BigInt(parsedArgs.targetBalance) + TX_FEE;

    console.log(`Unfreezing account ${item.address}`);
    const tx = await microwave.methods
      .deploy({
        dest: item.address,
        state_init: item.state,
      })
      .send({
        from: giverAddress,
        amount: amount.toString(),
        bounce: false,
      });
    console.log(`  waiting for tx ${tx.id.hash}`);
    await subscriber.trace(tx).finished();
    console.log(`  done`);
  }

  process.exit(0);
}

const addressRegex = /^(?:-1|0):[\da-fA-F]{64}$/;
const checkAddress = (address: string) => addressRegex.test(address);

async function guessGiverAccount(
  provider: ProviderRpcClient,
  address: Address,
  publicKey: string
): Promise<Account> {
  const { state } = await provider.getFullContractState({ address });
  if (state == null) {
    throw new Error("Giver account not found");
  }
  if (state.codeHash == null) {
    throw new Error("Giver account is uninit");
  }
  switch (state.codeHash) {
    case "84dafa449f98a6987789ba232358072bc0f76dc4524002a5d0918b9a75d2d599":
      return new WalletV3Account(address);
    case "0b3a887aeacd2a7d40bb5550bc9253156a029065aefb6d6b583735d58da9d5be":
      return new HighloadWalletV2(address);
    case "3ba6528ab2694c118180aa3bd10dd19ff400b909ab4dcf58fc69925b2c7b12a6":
      return new EverWalletAccount(address);
    case "ccbfc821853aa641af3813ebd477e26818b51e4ca23e5f6d34509215aa7123d9":
      return new GiverAccount({
        address,
        publicKey,
      });
    default:
      throw new Error("Unknown contract");
  }
}

async function readAccounts(path: string): Promise<Address[]> {
  const contents = await fs.readFile(path, {
    encoding: "utf8",
  });
  const lines = contents.split(/\r?\n/);
  const result = [];
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed != "") {
      result.push(new Address(trimmed));
    }
  }
  return result;
}

async function prepareStates(
  provider: ProviderRpcClient,
  accounts: Address[],
  ignoreCache: boolean
): Promise<FrozenAccount[]> {
  const applyState = async (address: Address): Promise<string> => {
    console.log(`  searching for freeze transaction`);
    let freezeTransactionLt: string | undefined;
    let continuation: TransactionId | undefined = undefined;
    outer: while (true) {
      const batch = await provider.getTransactions({
        address,
        continuation,
      });

      for (const transaction of batch.transactions) {
        if (transaction.endStatus !== "frozen") {
          throw new Error("Account not frozen");
        }
        if (transaction.origStatus !== "frozen") {
          freezeTransactionLt = transaction.id.lt;
          break outer;
        }
      }

      continuation = batch.continuation;
      if (continuation == null) {
        break;
      }
    }
    if (freezeTransactionLt == null) {
      throw new Error("Freeze transaction not found");
    }

    console.log(`  applying state at logical time ${freezeTransactionLt}`);
    const fullState = await fetch(`${STATES_API_URL}/apply`, {
      body: `{"account":"${address}","lt":${freezeTransactionLt}}`,
      headers: { "Content-Type": "application/json" },
      method: "POST",
    })
      .then(async (res) => {
        if (res.ok) {
          return res.json();
        } else {
          const e = await res.text();
          throw e;
        }
      })
      .then((res) => res.accountBoc);

    const stateInit = nt.parseFullAccountStateInit(fullState);
    if (stateInit == null) {
      throw new Error("Empty state");
    }
    return stateInit;
  };

  const cacheDirPath = path.join(os.tmpdir(), CACHE_DIR);
  await fs.mkdir(cacheDirPath, { recursive: true });

  const timestamp = TIMESTAMP_BIAS + ~~(clock.time / 1000);

  let current = 0;
  const total = accounts.length;
  const prefixPad = total.toString().length;
  const result = [];
  for (const address of accounts) {
    current += 1;
    const statePath = path.join(cacheDirPath, address.toString());

    console.log(
      `[${current
        .toString()
        .padStart(prefixPad)}/${total}] Preparing account ${address}`
    );
    try {
      const { state: accountState } = await provider.getFullContractState({
        address,
      });
      if (accountState == null) {
        throw new Error("Account is uninit");
      }

      const account = await provider.computeStorageFee({
        state: accountState,
        masterchain: address.toString().startsWith("-1"),
        timestamp,
      });
      if (
        account.accountStatus != "frozen" &&
        account.accountStatus != "nonexist"
      ) {
        throw new Error("Account is not frozen");
      }
      const storageFeeDebt =
        account.storageFeeDebt != null ? account.storageFeeDebt : "0";
      console.log(`  storage fee debt: ${storageFeeDebt}`);

      // Try to use cached state
      if (!ignoreCache) {
        const stateExists = await fs
          .stat(statePath)
          .then((stats) => stats.isFile())
          .catch((e) => {
            if (e.code === "ENOENT") {
              return false;
            } else {
              throw e;
            }
          });

        if (stateExists) {
          const data = await fs.readFile(statePath);
          result.push({
            address,
            state: data.toString("base64"),
            storageFeeDebt,
          } as FrozenAccount);
          console.log(`  using cached state`);
          continue;
        }
      }

      // Fallback to compute
      const state = await applyState(address);
      const binary = Buffer.from(state, "base64");
      await fs.writeFile(statePath, binary);
      result.push({
        address,
        state,
        storageFeeDebt,
      } as FrozenAccount);
      console.log(`  updated cached state`);
    } catch (e: any) {
      console.error(`  skipping: ${e.message}`);
    }
  }

  return result;
}

const MICROWAVE_BOC =
  "te6ccgEBCQEA3gACATQDAQEBwAIAQ9AAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACACKP8AIMEB9KQgWJL0oOBfAoog7VPZBgQBCvSkIPShBQAAAQPQQAcB/gHQ0wABwADysNYB0wAwwADyvDAgxwGa7UDtUHADXwPbMAHAAI5QMAHTH4IQU8UVHyIBuZcwwADyfPI84IIQU8UVHxK68ryCEDuaygBw+wLIgBDPCwUB+kACznD6AnbPC2sB1DABzMmBAID7ACBwcFkBVQFVAtkgWQFVAeAixwIIAAzAACIiAeI=";

const MICROWAVE_ABI = {
  "ABI version": 2,
  version: "2.2",
  functions: [
    {
      name: "deploy",
      inputs: [
        {
          name: "dest",
          type: "address",
        },
        {
          name: "state_init",
          type: "cell",
        },
      ],
      outputs: [],
    },
  ],
  events: [],
  headers: [],
} as const;

type Args = {
  positional: string[];
  flags: Record<string, string | true>;
};

const getArgs = () =>
  process.argv.slice(2, process.argv.length).reduce(
    (res, arg) => {
      if (arg.slice(0, 2) === "--") {
        const longArg = arg.split("=");
        const longArgFlag = longArg[0].slice(2, longArg[0].length);
        const longArgValue = longArg.length > 1 ? longArg[1] : true;
        res.flags[longArgFlag] = longArgValue;
      } else if (arg[0] === "-") {
        const flags = arg.slice(1, arg.length).split("");
        flags.forEach((flag) => {
          res.flags[flag] = true;
        });
      } else {
        res.positional.push(arg);
      }
      return res;
    },
    { positional: [], flags: {} } as Args
  );

const args = getArgs();

if (args.flags["h"] != null || args.flags["help"] != null) {
  printHelp();
  process.exit(1);
}

app(args).catch((e) => {
  console.log(e.message);
  process.exit(1);
});
