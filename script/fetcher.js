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
const parsedConcurrency = Number.parseInt(
  process.env.CONCURRENCY_LIMIT ?? "50",
  10,
);
const CONCURRENCY_LIMIT = 100;
const DEFAULT_CONCURRENCY =
  Number.isFinite(parsedConcurrency) && parsedConcurrency > 0
    ? Math.min(parsedConcurrency, CONCURRENCY_LIMIT)
    : Math.min(50, CONCURRENCY_LIMIT);
const FORCE_REFRESH_JSON = false;
const FORCE_REFRESH_FILES = false;
const CLEAN_TEMP_BEFORE_DOWNLOAD = true;

// --- NEW: Config for File Downloading ---
const DOWNLOAD_PATH = "/download-dok";
const DOWNLOAD_DIR = path.join(__dirname, "..", "temp_downloads"); // This is our file staging folder
// --- End Configuration ---

function parseNipListInput(input) {
  return input
    .split(/[\s,]+/)
    .map((value) => value.trim())
    .filter(Boolean);
}

function parseCliArgs(argv) {
  const options = {
    extraNipValues: [],
    extraNipFiles: [],
    useMasterList: true,
    forceJsonRefresh: FORCE_REFRESH_JSON,
    forceFileRefresh: FORCE_REFRESH_FILES,
    concurrency: DEFAULT_CONCURRENCY,
    help: false,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    switch (arg) {
      case "--extra-nips":
      case "--nips":
        if (i + 1 >= argv.length) {
          throw new Error(`${arg} requires a comma/space separated list of NIPs.`);
        }
        options.extraNipValues.push(argv[++i]);
        break;
      case "--extra-nips-file":
      case "--nips-file":
        if (i + 1 >= argv.length) {
          throw new Error(`${arg} requires a path to a file containing NIPs.`);
        }
        options.extraNipFiles.push(argv[++i]);
        break;
      case "--only-nips":
        options.useMasterList = false;
        break;
      case "--force-json":
        options.forceJsonRefresh = true;
        break;
      case "--force-files":
        options.forceFileRefresh = true;
        break;
      case "--concurrency":
        if (i + 1 >= argv.length) {
          throw new Error("--concurrency requires a numeric value.");
        }
        {
          const value = Number.parseInt(argv[++i], 10);
          if (!Number.isFinite(value) || value <= 0) {
            throw new Error("--concurrency must be a positive integer.");
          }
          options.concurrency = Math.min(value, CONCURRENCY_LIMIT);
        }
        break;
      case "--help":
      case "-h":
        options.help = true;
        break;
      default:
        throw new Error(`Unknown option: ${arg}`);
    }
  }

  return options;
}

async function resolveExtraNips(options) {
  const extraNips = new Set();

  for (const chunk of options.extraNipValues) {
    for (const nip of parseNipListInput(chunk)) {
      extraNips.add(nip);
    }
  }

  for (const filePath of options.extraNipFiles) {
    const absolutePath = path.resolve(process.cwd(), filePath);
    let contents;
    try {
      contents = await fs.readFile(absolutePath, "utf-8");
    } catch (err) {
      throw new Error(
        `Unable to read NIP list file "${filePath}": ${err.message}`,
      );
    }
    for (const nip of parseNipListInput(contents.replace(/\r/g, "\n"))) {
      extraNips.add(nip);
    }
  }

  return extraNips;
}

function printHelp() {
  const lines = [
    "Usage: node script/fetcher.js [options]",
    "",
    "Options:",
    "  --extra-nips \"NIP1,NIP2\"   Add specific NIPs (comma or space separated).",
    "  --extra-nips-file <path>    Load extra NIPs from file (comma/space/line separated).",
    "  --only-nips                 Ignore ms_employee.json; process only the provided NIPs.",
    "  --force-json                Re-fetch JSON even when a cached file exists.",
    "  --force-files               Re-download files even when already downloaded.",
    "  --concurrency <n>           Override concurrency (max 100).",
    "  --help                      Show this message.",
  ];
  lines.forEach((line) => logger.info(line));
}

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
 * Retries the provided request once when a 401 is encountered so we can refresh
 * the dynamic token without crashing the entire batch.
 * @param {(token: string) => Promise<*>} makeRequest
 * @param {{ current: string }} tokenRef
 * @param {string} context
 * @returns {Promise<*>}
 */
async function withTokenRetry(makeRequest, tokenRef, context) {
  try {
    return await makeRequest(tokenRef.current);
  } catch (error) {
    if (error.response && error.response.status === 401) {
      logger.warn(
        `[AUTH] Token expired during ${context}. Refreshing token and retrying once.`,
      );
      tokenRef.current = await fetchDynamicToken();
      return makeRequest(tokenRef.current);
    }
    throw error;
  }
}

/**
 * This function now does TWO things:
 * 1. Fetches and saves the NIP's JSON.
 * 2. Parses the JSON and downloads all associated files.
 */
async function fetchAndSaveAllData(
  nip,
  tokenRef,
  staticToken,
  {
    forceJsonRefresh = FORCE_REFRESH_JSON,
    forceFileRefresh = FORCE_REFRESH_FILES,
  } = {},
) {
  const jsonFilePath = path.join(STAGING_DIR, `${nip}.json`);

  // --- 1. JSON Handling ---
  let historyRecords = null;

  if (!forceJsonRefresh) {
    try {
      const existingJson = await fs.readFile(jsonFilePath, "utf-8");
      const parsed = JSON.parse(existingJson);
      if (parsed && Array.isArray(parsed.data)) {
        historyRecords = parsed.data;
        logger.info(`[JSON CACHE] Using cached ${nip}.json.`);
      } else {
        logger.warn(
          `[JSON CACHE] Cached ${nip}.json missing 'data' array. Fetching from API.`,
        );
      }
    } catch (e) {
      if (e.code !== "ENOENT") {
        logger.warn(
          `[JSON CACHE] Unable to read cached ${nip}.json (${e.message}). Fetching from API.`,
        );
      }
    }
  } else {
    try {
      await fs.unlink(jsonFilePath);
      logger.info(
        `[REFRESH JSON] Removed existing ${nip}.json before refetch.`,
      );
    } catch (e) {
      if (e.code !== "ENOENT") {
        logger.warn(
          `[REFRESH JSON] Failed removing cached ${nip}.json (${e.message}).`,
        );
      }
    }
  }

  const needsJsonFetch = historyRecords === null;

  // --- 2. Fetch and Save JSON when needed ---
  const makeAuthHeaders = (token) => ({
    accept: "application/json",
    Authorization: `Bearer ${token}`,
    Auth: `Bearer ${staticToken}`,
  });

  if (needsJsonFetch) {
    try {
      logger.info(`[FETCH JSON] Fetching history for ${nip}...`);
      const url = `${API_BASE_URL}/jabatan/pns/${nip}`;
      const response = await withTokenRetry(
        (token) => axios.get(url, { headers: makeAuthHeaders(token) }),
        tokenRef,
        `JSON fetch for ${nip}`,
      );

      const data = response.data;
      await fs.writeFile(jsonFilePath, JSON.stringify(data, null, 2));
      logger.info(`[SAVE JSON] Successfully saved ${nip}.json`);

      historyRecords = data.data;
    } catch (error) {
      let errorMsg = error.message;
      if (error.response) {
        errorMsg = `Status ${error.response.status}: ${JSON.stringify(error.response.data)}`;
      }
      logger.error(`[FAIL JSON] Failed to process ${nip}: ${errorMsg}`);
      return; // Stop processing this NIP if JSON fails
    }
  }

  if (!historyRecords || !Array.isArray(historyRecords)) {
    logger.warn(`[PARSE] No history records array found for ${nip}.`);
    return;
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
      const safeFilename = `${record.id}_${docKey}_${path.basename(filePath)}`;
      const localFilePath = path.join(DOWNLOAD_DIR, safeFilename);

      // --- File "Resume" Logic ---
      if (!forceFileRefresh) {
        try {
          await fs.access(localFilePath);
          logger.warn(`[SKIP FILE] File ${safeFilename} already exists.`);
          continue; // Skip this file
        } catch (e) {
          // File does not exist, proceed to download.
        }
      } else if (CLEAN_TEMP_BEFORE_DOWNLOAD) {
        try {
          await fs.unlink(localFilePath);
          logger.info(
            `[REFRESH FILE] Removed old ${safeFilename} before download.`,
          );
        } catch (e) {
          // Ignore if file missing.
        }
      }

      // --- Download Logic ---
      try {
        logger.info(
          `[DOWNLOAD] Downloading: ${fileInfo.dok_nama} (NIP: ${nip})`,
        );
        await withTokenRetry(
          async (token) => {
            const fileResponse = await axios.get(downloadUrl, {
              headers: { ...makeAuthHeaders(token), accept: "application/pdf" },
              responseType: "stream",
            });

            const writer = fss.createWriteStream(localFilePath);
            fileResponse.data.pipe(writer);
            await new Promise((resolve, reject) => {
              writer.on("finish", resolve);
              writer.on("error", reject);
            });
          },
          tokenRef,
          `file download for ${nip} (${safeFilename})`,
        );

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
  let cliOptions;
  try {
    cliOptions = parseCliArgs(process.argv.slice(2));
  } catch (err) {
    logger.error(`[ARGS] ${err.message}`);
    printHelp();
    process.exitCode = 1;
    return;
  }

  if (cliOptions.help) {
    printHelp();
    return;
  }

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

  let extraNipSet;
  try {
    extraNipSet = await resolveExtraNips(cliOptions);
  } catch (err) {
    logger.error(`[ARGS] ${err.message}`);
    process.exitCode = 1;
    return;
  }

  const nipSet = new Set();
  if (cliOptions.useMasterList) {
    MASTER_NIP_LIST.forEach((nip) => nipSet.add(nip));
  }
  extraNipSet.forEach((nip) => nipSet.add(nip));

  if (!cliOptions.useMasterList && nipSet.size === 0) {
    logger.error(
      "[ARGS] --only-nips was specified but no additional NIPs were provided.",
    );
    process.exitCode = 1;
    return;
  }

  if (nipSet.size === 0) {
    logger.warn("No NIPs to process after applying filters. Exiting.");
    return;
  }

  const tokenRef = { current: await fetchDynamicToken() };
  const staticToken = STATIC_AUTH_TOKEN;

  if (!tokenRef.current) return;

  const finalNipList = Array.from(nipSet);
  const concurrency = cliOptions.concurrency;
  const fetchOptions = {
    forceJsonRefresh: cliOptions.forceJsonRefresh,
    forceFileRefresh: cliOptions.forceFileRefresh,
  };

  logger.info(`--- Starting batch processing ---`);
  logger.info(
    `Total NIPs to process: ${finalNipList.length} (master list ${cliOptions.useMasterList ? MASTER_NIP_LIST.length : 0}, extra ${extraNipSet.size})`,
  );
  logger.info(
    `Concurrency set to: ${concurrency} (max ${CONCURRENCY_LIMIT})`,
  );
  if (cliOptions.forceJsonRefresh) {
    logger.info("[CONFIG] JSON refresh forced for all NIPs.");
  }
  if (cliOptions.forceFileRefresh) {
    logger.info("[CONFIG] File downloads forced for all NIPs.");
  }

  const queue = [...finalNipList];

  while (queue.length > 0) {
    const batchNIPs = queue.splice(0, concurrency);
    const promises = batchNIPs.map((nip) =>
      // Renamed the function to be more descriptive
      fetchAndSaveAllData(nip, tokenRef, staticToken, fetchOptions),
    );
    await Promise.all(promises);
    logger.info(`--- Batch complete. ${queue.length} NIPs remaining. ---`);
  }

  logger.info("--- Phase 1: Fetcher Script Finished ---");
}

main();
