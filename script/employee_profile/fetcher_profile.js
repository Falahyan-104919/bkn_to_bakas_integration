require("dotenv").config();
const fs = require("fs").promises; // Use promises for async
const fss = require("fs"); // Use non-promise 'fs' for createWriteStream
const path = require("path");
const axios = require("axios");
const { URLSearchParams } = require("url");
const logger = require("../logger");

// --- Configuration ---
const API_BASE_URL = process.env.API_BASE_URL;
const TOKEN_URL = process.env.TOKEN_URL;
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const STATIC_AUTH_TOKEN = process.env.STATIC_AUTH_TOKEN;

const masterEmployee = require("../../ms_employee.json");
const MASTER_NIP_LIST = masterEmployee.map((emp) => emp.employee_nip);
const STAGING_DIR = path.join(__dirname, "staging_employee");
const CONCURRENCY = 100;

async function getExistingStagingNips() {
  const stagedNips = new Set();

  try {
    const files = await fs.readdir(STAGING_DIR);
    for (const fileName of files) {
      if (path.extname(fileName) !== ".json") continue;
      const nip = path.basename(fileName, ".json");
      if (nip) stagedNips.add(nip);
    }
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }

  return stagedNips;
}

async function fetchDynamicToken() {
  logger.info(`[AUTH] Requesting new dynamic token from : ${TOKEN_URL}`);
  const body = new URLSearchParams();
  body.append("grant_type", "client_credentials");
  body.append("client_id", CLIENT_ID);
  body.append("client_secret", CLIENT_SECRET);

  try {
    const response = await axios.post(TOKEN_URL, body, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
    });
    return response.data.access_token;
  } catch (error) {
    logger.error("[AUTH] ❌ FAILED to fetch dynamic token:");
    if (error.response) {
      logger.error(`Status: ${error.response.status}`);
      logger.error(`Data: ${JSON.stringify(error.response.data)}`);
    } else {
      logger.error(`Error: ${error.message}`);
    }
    throw new Error("Could not fetch dynamic token. Stopping script.");
  }
}

async function withTokenRetry(makeRequest, tokenRef, context) {
  try {
    return await makeRequest(tokenRef.current);
  } catch (error) {
    if (error.response && error.response.status === 401) {
      logger.warn(`[AUTH] Token expired during ${context}. Refreshing token and retrying once.`);
      tokenRef.current = await fetchDynamicToken();
      return makeRequest(tokenRef.current);
    }
    throw error;
  }
}

async function fetchEmployeeProfile(nip, tokenRef, staticToken) {
  const jsonFilePath = path.join(STAGING_DIR, `${nip}.json`);

  const makeAuthHeaders = (token) => ({
    accept: "application/json",
    Authorization: `Bearer ${token}`,
    Auth: `Bearer ${staticToken}`,
  });

  try {
    logger.info(`[FETCH JSON] Fetching history for ${nip}...`);
    const url = `${API_BASE_URL}/pns/data-utama/paruhwaktu/${nip}`;
    const response = await withTokenRetry((token) => axios.get(url, { headers: makeAuthHeaders(token) }), tokenRef, `JSON fetch for ${nip}`);

    const data = response.data;
    await fs.writeFile(jsonFilePath, JSON.stringify(data, null, 2));
    logger.info(`[SAVE JSON] Successfully saved ${nip}.json`);
  } catch (error) {
    let errorMsg = error.message;
    if (error.response) {
      errorMsg = `STATUS ${error.response.status} : ${JSON.stringify(error.response.data)}`;
    }
    logger.error(`[FAIL JSON] Failed to process ${nip}: ${errorMsg}`);
    return;
  }
}

async function main() {
  if (!API_BASE_URL || !TOKEN_URL || !CLIENT_ID || !CLIENT_SECRET || !STATIC_AUTH_TOKEN) {
    logger.error("--- ❌ FAILED! ---");
    logger.error("Error: One or more required variables are missing from .env.");
    logger.error("--- Script Aborted ---");
    return;
  }

  await fs.mkdir(STAGING_DIR, { recursive: true });

  const tokenRef = { current: await fetchDynamicToken() };
  const staticToken = STATIC_AUTH_TOKEN;

  if (!tokenRef.current) return;

  logger.info(`--- Starting batch processing ---`);
  const existingStagingNips = await getExistingStagingNips();
  const queue = MASTER_NIP_LIST.filter((nip) => !existingStagingNips.has(nip));

  logger.info(`Total NIPs in master data: ${MASTER_NIP_LIST.length}`);
  logger.info(`Skipped (already in staging_employee): ${MASTER_NIP_LIST.length - queue.length}`);
  logger.info(`Total NIPs to fetch: ${queue.length}`);

  if (queue.length === 0) {
    logger.info("--- No new NIPs to fetch. Script finished. ---");
    return;
  }

  while (queue.length > 0) {
    const batchNIPs = queue.splice(0, CONCURRENCY);
    const promises = batchNIPs.map((nip) => fetchEmployeeProfile(nip, tokenRef, staticToken));
    await Promise.all(promises);

    logger.info(`--- Batch complete. ${queue.length} NIPs remaining. ---`);
  }
}

main();
