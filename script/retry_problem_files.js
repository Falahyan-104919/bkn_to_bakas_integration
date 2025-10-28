#!/usr/bin/env node
require("dotenv").config();

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const axios = require("axios");
const { URLSearchParams } = require("url");
const logger = require("./logger");

// === 1. Configuration =======================================================
const FINAL_JSON_PATH = path.join(
  __dirname,
  "..",
  "staging_data",
  "1-final.json", // <-- adjust if your file lives elsewhere
);
const DOWNLOAD_DIR = path.join(__dirname, "..", "temp_downloads");
const DOWNLOAD_PATH = "/download-dok";

const PROBLEM_RECORD_IDS = [
  "8ae48289367d13ed01369b7cb8ad0986",
  "83c59405-eebd-4b79-8f08-1b30ea9c9495",
  "A8ACA8CA3F623912E040640A040269BB",
  "8ae482875b3fb608015b40d3c3613ba1",
  "8ae482855a8e22f9015a8e7300eb194f",
  "8ae4829d5707f16301570808a9271ba5",
  "A8ACA8EABF313912E040640A040269BB",
  "A8ACA8F65A893912E040640A040269BB",
  "8ae482a450ae439f0150bc959f745626",
  "A8ACA8D1B3043912E040640A040269BB",
  "e241a2e8-0d1b-4f85-8487-f5fef420ca20",
  "8ae482a6516684e30151701216ca5d58",
  "8ae482a550f466440150f4fc05131f6b",
  "8ae483a85e0f27c5015e185cfcd13804",
  "A8ACA8F576433912E040640A040269BB",
  "8ae482a750ae3caa0150bba4595c0f21",
  "A8ACA8EAD7E43912E040640A040269BB",
  "8ae4820650ae3c970150c17c73395904",
  "ff80808131b913500131d071339b6411",
  "8ae4820650dc89fc0150e7162ba87d70",
  "8ae482035101398001510e1a923a7429",
  "8ae4820350f46b3b0150f55dc09633b3",
  "8ae482035148cd9801514c4714a84abe",
  "ff8080813639eb0401363d9e944b1711",
  "d9285a8a-0356-46dc-a5f9-c43d01c93389",
  "8ae482a650ae43df0150c24defa46168",
  "8ae4820350863e0a0150890ad5583b01",
  "8ae483a56a04ebb6016a0569c66e414a",
  "e41c1a61-20c7-4be3-b0a1-df4fb6171dd6",
  "ff80808152eed58e015306c8f3cc0bd0",
  "8778ebfb-65a0-4d95-a014-b1acf1033804",
  "8ae482875a624453015a64953b597fb5",
  "ff8080814aa9479e014ab960ed12388b",
  "8ae483a56cdc1f9b016cf5d860fc2deb",
  "ff8080813daef8f5013db0635dfc023d",
  "ff808081449546150144965848990813",
  "cc51624a-e2a1-4c1f-92a0-8bb0577dfd75",
  "ff8080813d207bfc013d3485ec71488c",
  "8ae482875c4133b5015c43b35bd74686",
  "8ae483a861d2fef10161d6081dd3351e",
  "ff8080815626fe5f015629f9ba651dc6",
  "8ae483a55e2632a9015e264f1d760e7b",
  "8ae483a66c4d65a9016c4d7e457101d3",
  "ff8080815626fe5f015629e0cdac71fa",
  "8ae483c6765c866901766a3afc7e39e9",
  "8ae483a5765c838501766a1efc5871cd",
  "8ae483c671aa7a8d0171b382cbd56922",
  "8ae4829d4fbbbb57014fc97bef172058",
  "A8ACA8EDC7F43912E040640A040269BB",
  "8ae482884fcff101014fd3b635ad2463",
  "8ae482035124bfd501512ac90bb70f05",
  "ff808081370690e4013706d745c90cf9",
  "A8ACA8E261463912E040640A040269BB",
  "ff80808156c1fd380156c53372f641f6",
  "ff808081395105e601396bef85820e9a",
  "ff80808153354d38015340657d772fe8",
  "9ead8e5e-d0fe-4da7-80f2-0783176090e6",
  "ff8080813199f3870131ad67add33a7b",
  "8ae483a66268745c01626f3f77a25c08",
  "8ae482a650970d4d015097bd466b6933",
  "ff808081449546150144965d63190fbe",
  "8ae482a65156933d01515b10cd271c14",
  "8ae483c57ba477db017bac1d8bfa6dca",
  "8ae482a450c38b720150c396dc330a39",
  "8ae483a85e263302015e27709eaf00a6",
  "ff8080813970446701397123fde030af",
  "8ae482a4518b76790151a8aacc573739",
  "8ae482a7511569470151188e6e9848db",
  "e6a5ac98-003f-4cae-a203-33a1d65f63dc",
  "96e9848f-075c-490c-9fef-a334691be297",
  "ff8080813d53c628013d62a6ba524399",
  "8ae482855a6507e7015a654046fb14b9",
  "8ae483a569d796cc0169eba98fe538e0",
  "ff808081513cb660015146dd7a910ba1",
  "8ae483a861d2fef10161d601617d2f26",
  "8ae483c6714d8db201715a17a3e908fa",
  "8aca5afd-8d6b-4076-9357-f2047daf4ac6",
  "ff8080813d53c628013d62b281db4ede",
  "ff80808140cec7d80140e71e58275ed5",
  "ff80808139704467013971c5fcca50e2",
  "8ae482a75124b31301512979ae0e53bb",
  "8ae483a57fbab3c7017fbe5dbc81061b",
  "69910313-fd9e-4c5e-80fe-bd1c57bf6122",
  "8ae482875a624453015a648fbba972f3",
  "8ae483c577b948e10177c5101e480bef",
  "8ae483a67b5c74d6017b61aaad5a0a94",
  "8ae483c681d266930181d674d5033d48",
  "8ae483a55e2632a9015e265c0d2e18fb",
  "8ae483a765a13ad20165cc38594d6e8b",
  "8ae483a55e6d4dff015e6ed6931b00cf",
  "ff80808139704467013971211ae62f4f",
  "ff8080813639eb0401363dbb8e801c15",
  "8ae4829d52e520330152e838de0d04af",
  "8ae483a861b7ef6a0161c0070f4f2c1f",
  "8ae483a561e7703b016204bda528531f",
  "8ae483a765648a0e016564d6486b5809",
  "8ae483a67b5c74d6017b61b3df0d100c",
  "8ae483c57ba477db017bac24f0466e93",
  "8ae482a75a8e2296015a9814c47979b5",
  "8ae48286574fc6c7015750279aa12c84",
  "8ae483a66652ce7b0166803f1cec5a98",
  "ff80808139704467013971be8392506d",
  "fceb0156-855c-49d6-ac7d-d88d996c5587",
  "b6b52714-1a32-4ca3-957c-9012e026cc29",
  "d5783062-2462-4f4c-9804-19a3236c27ae",
  "f7ec2508-b65c-4e4d-ba0d-35f3c764844c",
  "9383d839-b134-41c8-bbf3-b94d06d73d36",
  "ff80808140cec7d80140e704e3a94777",
  "ff8080814495461501449f9013af690c",
  "8ae483c57ba477db017bac33b7446fea",
  "f8e1a751-d6c9-49dd-bd4c-8bdf79a6b79b",
  "8ae483c6714d8db201715a186c1d091d",
  "ff8080815740b2bf015740f73b8d18b4",
  "8ae483a56cdc1f9b016cef9ac0780f5d",
  "8ae482a550ae43b50150bc5ec6b4355b",
  "5906d6ad-44bb-4ad2-b3f7-7fe84f36bddc",
  "8ae483c574e9e50c0174f75081866cc2",
  "ff808081568c4d1801568dca901f268c",
  "8ae483c57e666ead017e868da1f214dc",
  "8ae483a561d2f2050161d6315df93966",
  "ff80808144900bc5014496c5fd624508",
  "8ae482893dd4a22c013dec9ef6de7a74",
  "8ae483a66615d508016632e05ae8349c",
  "5ad727e7-a781-4ff7-906f-6ceaabf0d3fd",
  "73c040c7-5d13-4d34-846a-798927caaea4",
  "8ae483a85e0f27c5015e18708bc349ac",
  "8ae483a561d761b20161dc4988222bd5",
  "8ae483c57ba477db017bac3eadf1729b",
  "8ae483c671aa7a8d0171b39126f769bc",
  "8ae483a56cdc1f9b016cf5e4f7f23ef8",
  "8ae483c67836b2f6017839c761607835",
  "8ae4829d52e520330152e82b03a26b6d",
  "21e9fa09-97ef-48cc-9bce-30843f3bdaca",
  "ff80808144d3aa4b0144d96419f04071",
  "8ae483a861d2fef10161d5fc713a2a4d",
  "8ae483a67b5c74d6017b61d72f1d2178",
  "8ae482a759b076c20159ba2038fc4349",
  "8ae483a861d2fef10161d6081dd3351e",
  "8ae4828944957bc601449bffd2d34e25",
  "8ae483a581fb489a0181fcab92073f8f",
  "8ae483a57b7ad139017b7b4ba6122d7b",
  "8ae483c6718161ff017192d0555f0096",
  "c5f93647-ff53-40ce-9894-22527fb92bcd",
  "8ae483c581d1f47e0181eb5e54cd0733",
  "8ae483c581f0d7fb0181f575f63c701c",
  "8ae483a57f437fce017f59aee15f32a4",
];

const API_BASE_URL = process.env.API_BASE_URL;
const TOKEN_URL = process.env.TOKEN_URL;
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const STATIC_AUTH_TOKEN = process.env.STATIC_AUTH_TOKEN;

// === 2. Token handling ======================================================
async function fetchDynamicToken() {
  const body = new URLSearchParams();
  body.append("grant_type", "client_credentials");
  body.append("client_id", CLIENT_ID);
  body.append("client_secret", CLIENT_SECRET);

  logger.info("[AUTH] Requesting new OAuth token…");
  const response = await axios.post(TOKEN_URL, body, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });
  logger.info("[AUTH] Token acquired.");
  return response.data.access_token;
}

async function withTokenRetry(makeRequest, tokenRef, ctx) {
  try {
    return await makeRequest(tokenRef.current);
  } catch (error) {
    if (error.response && error.response.status === 401) {
      logger.warn(`[AUTH] Token expired during ${ctx}. Refreshing…`);
      tokenRef.current = await fetchDynamicToken();
      return makeRequest(tokenRef.current);
    }
    throw error;
  }
}

function authHeaders(token) {
  return {
    accept: "*/*",
    Authorization: `Bearer ${token}`,
    Auth: `Bearer ${STATIC_AUTH_TOKEN}`,
  };
}

// === 3. Helpers =============================================================
async function loadRecordIndex() {
  const raw = await fsp.readFile(FINAL_JSON_PATH, "utf-8");
  const parsed = JSON.parse(raw);

  const recordsArray = Array.isArray(parsed.data)
    ? parsed.data
    : Array.isArray(parsed)
      ? parsed
      : [];

  if (recordsArray.length === 0) {
    throw new Error(
      `Unable to find an array of records in ${FINAL_JSON_PATH}. Check file shape.`,
    );
  }

  const map = new Map();
  for (const record of recordsArray) {
    if (record && record.id) {
      map.set(record.id, record);
    }
  }
  logger.info(`[LOAD] Indexed ${map.size} records from ${FINAL_JSON_PATH}`);
  return map;
}

function safeTempName(recordId, docKey, dokUri) {
  const basename = path.basename(dokUri || "");
  if (!basename) {
    throw new Error(
      `Record ${recordId} doc ${docKey} has no basename inside dok_uri.`,
    );
  }
  return `${recordId}_${docKey}_${basename}`;
}

async function downloadAndSaveFile({ record, docKey, fileInfo }, tokenRef) {
  const encodedPath = encodeURIComponent(fileInfo.dok_uri);
  const downloadUrl = `${API_BASE_URL}${DOWNLOAD_PATH}?filePath=${encodedPath}`;
  const safeName = safeTempName(record.id, docKey, fileInfo.dok_uri);
  const localPath = path.join(DOWNLOAD_DIR, safeName);
  const tempPath = `${localPath}.tmp`;

  logger.info(`[DL] ${record.id} doc ${docKey} → ${safeName}`);

  // make sure target dir exists and previous artifacts are gone
  await fsp.mkdir(DOWNLOAD_DIR, { recursive: true });
  await Promise.allSettled([fsp.unlink(localPath), fsp.unlink(tempPath)]);

  const response = await withTokenRetry(
    (token) =>
      axios.get(downloadUrl, {
        responseType: "stream",
        headers: authHeaders(token),
      }),
    tokenRef,
    `download ${record.id} doc ${docKey}`,
  );

  await new Promise((resolve, reject) => {
    const writer = fs.createWriteStream(tempPath);
    response.data.pipe(writer);
    writer.on("finish", resolve);
    writer.on("error", reject);
  });

  const stats = await fsp.stat(tempPath);
  if (stats.size === 0) {
    throw new Error(`Downloaded file is 0 bytes (${safeName})`);
  }

  await fsp.rename(tempPath, localPath);
  logger.info(`[OK] Saved ${safeName} (${stats.size} bytes)`);
}

// === 4. Main =================================================================
async function main() {
  const recordIndex = await loadRecordIndex();
  const tokenRef = { current: await fetchDynamicToken() };

  let success = 0;
  let skipped = 0;
  let failed = 0;

  for (const recordId of PROBLEM_RECORD_IDS) {
    const record = recordIndex.get(recordId);
    if (!record) {
      skipped += 1;
      logger.error(`[MISS] Record ${recordId} not found in dataset; skipping.`);
      continue;
    }

    if (!record.path || Object.keys(record.path).length === 0) {
      skipped += 1;
      logger.error(`[MISS] Record ${recordId} has no path entries; skipping.`);
      continue;
    }

    for (const [docKey, fileInfo] of Object.entries(record.path)) {
      try {
        await downloadAndSaveFile({ record, docKey, fileInfo }, tokenRef);
        success += 1;
      } catch (error) {
        failed += 1;
        logger.error(`[FAIL] ${recordId} doc ${docKey}: ${error.message}`);
      }
    }
  }

  logger.info(
    `[RESULT] Downloads complete. success=${success}, skipped=${skipped}, failed=${failed}`,
  );
  if (failed > 0) process.exitCode = 1;
}

if (require.main === module) {
  main().catch((error) => {
    logger.error(`[FATAL] ${error.message}`);
    logger.error(error.stack);
    process.exit(1);
  });
}
