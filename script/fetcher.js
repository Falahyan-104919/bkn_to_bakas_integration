require("dotenv").config();
const fs = require("fs").promises; // Use promises for async
const fss = require("fs"); // Use non-promise 'fs' for createWriteStream
const path = require("path");
const axios = require("axios");
const { URLSearchParams } = require("url");
const logger = require("./logger");

// --- Configuration ---
const API_BASE_URL = process.env.API_BASE_URL;
const TOKEN_URL = process.env.TOKEN_URL;
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const STATIC_AUTH_TOKEN = process.env.STATIC_AUTH_TOKEN;

// Config for JSON staging
const masterEmployeeData = require("../ms_employee.json");
const MASTER_NIP_LIST = masterEmployeeData.map((emp) => emp.employee_nip);
const STAGING_DIR = path.join(__dirname, "..", "staging_data");
const CONCURRENCY_LIMIT = 10;

// --- NEW: Config for File Downloading ---
const DOWNLOAD_PATH = "/download-dok";
const DOWNLOAD_DIR = path.join(__dirname, "..", "temp_downloads"); // This is our file staging folder
// --- End Configuration ---

async function fetchDynamicToken() {
  logger.info(`[AUTH] Requesting new dynamic token from: ${TOKEN_URL}`);
  const body = new URLSearchParams();
  body.append("grant_type", "client_credentials");
  body.append("client_id", CLIENT_ID);
  body.append("client_secret", CLIENT_SECRET);
  try {
    const response = await axios.post(TOKEN_URL, body, {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
    });
    logger.info("[AUTH] Successfully fetched dynamic token.");
    return response.data.access_token;
  } catch (error) {
    // ... (error logging as before) ...
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

/**
 * This function now does TWO things:
 * 1. Fetches and saves the NIP's JSON.
 * 2. Parses the JSON and downloads all associated files.
 */
async function fetchAndSaveAllData(nip, dynamicToken, staticToken) {
  const jsonFilePath = path.join(STAGING_DIR, `${nip}.json`);

  // --- 1. JSON "Resume" Logic ---
  try {
    await fs.access(jsonFilePath);
    logger.warn(`[SKIP] ${nip}.json already exists. Skipping NIP.`);
    return; // This skips both JSON and file downloads
  } catch (e) {
    // File does not exist, proceed.
  }

  // --- 2. Fetch and Save JSON ---
  let historyRecords; // We need this for Step 3
  const authHeaders = {
    accept: "application/json",
    Authorization: `Bearer ${dynamicToken}`,
    Auth: `Bearer ${staticToken}`,
  };

  try {
    logger.info(`[FETCH JSON] Fetching history for ${nip}...`);
    const url = `${API_BASE_URL}/jabatan/pns/${nip}`;
    const response = await axios.get(url, { headers: authHeaders });

    const data = response.data;
    await fs.writeFile(jsonFilePath, JSON.stringify(data, null, 2));
    logger.info(`[SAVE JSON] Successfully saved ${nip}.json`);

    historyRecords = data.data; // Get the array for the next step

    if (!historyRecords || !Array.isArray(historyRecords)) {
      logger.warn(`[PARSE] No history records array found for ${nip}.`);
      return; // No files to download
    }
  } catch (error) {
    let errorMsg = error.message;
    if (error.response) {
      errorMsg = `Status ${error.response.status}: ${JSON.stringify(error.response.data)}`;
    }
    logger.error(`[FAIL JSON] Failed to process ${nip}: ${errorMsg}`);
    return; // Stop processing this NIP if JSON fails
  }

  // --- 3. Download Associated Files ---
  logger.info(`[FETCH FILES] Checking for files for ${nip}...`);
  for (const record of historyRecords) {
    if (!record.path || Object.keys(record.path).length === 0) {
      continue; // No files for this specific record
    }

    logger.info(`[FETCH FILES] Found files for record ${record.id}.`);
    for (const docKey in record.path) {
      const fileInfo = record.path[docKey];
      const filePath = fileInfo.dok_uri;
      const encodedFilePath = encodeURIComponent(filePath);
      const downloadUrl = `${API_BASE_URL}${DOWNLOAD_PATH}?filePath=${encodedFilePath}`;

      // This is the file-staging name we agreed on
      const safeFilename = `${record.id}_${fileInfo.dok_id}_${path.basename(filePath)}`;
      const localFilePath = path.join(DOWNLOAD_DIR, safeFilename);

      // --- File "Resume" Logic ---
      try {
        await fs.access(localFilePath);
        logger.warn(`[SKIP FILE] File ${safeFilename} already exists.`);
        continue; // Skip this file
      } catch (e) {
        // File does not exist, proceed to download.
      }

      // --- Download Logic ---
      try {
        logger.info(
          `[DOWNLOAD] Downloading: ${fileInfo.dok_nama} (NIP: ${nip})`,
        );
        const fileResponse = await axios.get(downloadUrl, {
          headers: { ...authHeaders, accept: "application/pdf" },
          responseType: "stream",
        });

        const writer = fss.createWriteStream(localFilePath);
        fileResponse.data.pipe(writer);
        await new Promise((resolve, reject) => {
          writer.on("finish", resolve);
          writer.on("error", reject);
        });

        logger.info(`[SAVE FILE] Saved file to: ${localFilePath}`);
      } catch (fileError) {
        logger.error(
          `[FAIL FILE] Failed to download ${fileInfo.dok_nama} (NIP: ${nip})`,
        );
        if (fileError.response) {
          logger.error(`Status: ${fileError.response.status}`);
        } else {
          logger.error(fileError.message);
        }
      }
    }
  }
}

async function main() {
  logger.info("--- Starting Phase 1: Fetcher Script ---");

  if (
    !API_BASE_URL ||
    !TOKEN_URL ||
    !CLIENT_ID ||
    !CLIENT_SECRET ||
    !STATIC_AUTH_TOKEN
  ) {
    logger.error("--- ❌ FAILED! ---");
    logger.error(
      "Error: One or more required variables are missing from .env.",
    );
    logger.error("--- Script Aborted ---");
    return;
  }

  // Create BOTH staging directories
  await fs.mkdir(STAGING_DIR, { recursive: true });
  await fs.mkdir(DOWNLOAD_DIR, { recursive: true }); // <-- NEW

  const dynamicToken = await fetchDynamicToken();
  const staticToken = STATIC_AUTH_TOKEN;

  if (!dynamicToken) return;

  logger.info(`--- Starting batch processing ---`);
  logger.info(`Total NIPs to process: ${MASTER_NIP_LIST.length}`);
  logger.info(`Concurrency limit set to: ${CONCURRENCY_LIMIT}`);

  const queue = [...MASTER_NIP_LIST];

  while (queue.length > 0) {
    const batchNIPs = queue.splice(0, CONCURRENCY_LIMIT);
    const promises = batchNIPs.map((nip) =>
      // Renamed the function to be more descriptive
      fetchAndSaveAllData(nip, dynamicToken, staticToken),
    );
    await Promise.all(promises);
    logger.info(`--- Batch complete. ${queue.length} NIPs remaining. ---`);
  }

  logger.info("--- Phase 1: Fetcher Script Finished ---");
}

main();
