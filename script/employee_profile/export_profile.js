require("dotenv").config();
const fs = require("fs").promises;
const path = require("path");
const axios = require("axios");

const logger = require("../logger");

const masterEmployee = require("../../ms_employee.json");

const EXPORT_URL =
  process.env.EXPORT_PROFILE_URL ||
  "http://36.91.222.106:3000/transaksi/jabatan/import-from-bkn";
const ACCESS_TOKEN = process.env.EXPORT_ACCESS_TOKEN;
const STAGING_DIR = path.resolve(__dirname, "staging_employee");
const CHECKPOINT_DIR = path.resolve(__dirname, "checkpoints");
const CHECKPOINT_FILE = path.join(CHECKPOINT_DIR, "export_profile_progress.json");
const REQUEST_TIMEOUT_MS = Number.parseInt(
  process.env.EXPORT_PROFILE_TIMEOUT_MS || "30000",
  10,
);
const CONCURRENCY = Number.parseInt(
  process.env.EXPORT_PROFILE_CONCURRENCY || "5",
  10,
);

function getMasterNips() {
  return masterEmployee
    .map((employee) => employee?.employee_nip)
    .filter((nip) => typeof nip === "string" && nip.trim().length > 0);
}

async function ensureCheckpointDir() {
  await fs.mkdir(CHECKPOINT_DIR, { recursive: true });
}

async function loadCheckpoint() {
  try {
    const raw = await fs.readFile(CHECKPOINT_FILE, "utf-8");
    const parsed = JSON.parse(raw);

    if (!parsed || typeof parsed !== "object" || typeof parsed.records !== "object") {
      return {
        version: 1,
        updatedAt: null,
        records: {},
      };
    }

    return {
      version: 1,
      updatedAt: parsed.updatedAt || null,
      records: parsed.records,
    };
  } catch (error) {
    if (error.code === "ENOENT") {
      return {
        version: 1,
        updatedAt: null,
        records: {},
      };
    }

    throw error;
  }
}

async function saveCheckpoint(checkpoint) {
  checkpoint.updatedAt = new Date().toISOString();
  await fs.writeFile(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
}

function buildCheckpointEntry(status, detail) {
  return {
    status,
    detail,
    updatedAt: new Date().toISOString(),
  };
}

function getPendingNips(nips, checkpoint) {
  return nips.filter((nip) => checkpoint.records[nip]?.status !== "success");
}

async function readPayloadFromStaging(nip) {
  const filePath = path.join(STAGING_DIR, `${nip}.json`);

  try {
    const raw = await fs.readFile(filePath, "utf-8");
    const parsed = JSON.parse(raw);

    if (!parsed || typeof parsed !== "object" || !parsed.data) {
      return {
        ok: false,
        reason: "invalid_payload_structure",
        detail: `invalid payload structure in ${filePath}`,
      };
    }

    if (typeof parsed.data !== "object" || Array.isArray(parsed.data)) {
      return {
        ok: false,
        reason: "invalid_payload_data",
        detail: "payload.data is not a valid object",
      };
    }

    return {
      ok: true,
      payload: parsed.data,
    };
  } catch (error) {
    if (error.code === "ENOENT") {
      return {
        ok: false,
        reason: "staging_file_not_found",
        detail: "staging file not found",
      };
    }

    return {
      ok: false,
      reason: "staging_file_read_error",
      detail: `failed to read staging file - ${error.message}`,
    };
  }
}

async function postProfile(nip, payload) {
  try {
    const response = await axios.post(EXPORT_URL, payload, {
      timeout: REQUEST_TIMEOUT_MS,
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: `Bearer ${ACCESS_TOKEN}`,
      },
    });

    return {
      ok: true,
      detail: `success with status ${response.status}`,
    };
  } catch (error) {
    if (error.response) {
      return {
        ok: false,
        reason: "request_failed",
        detail: `request failed with status ${error.response.status} - ${JSON.stringify(error.response.data)}`,
      };
    }

    return {
      ok: false,
      reason: "request_error",
      detail: `request error - ${error.message}`,
    };
  }
}

async function processNip(nip) {
  const stagingResult = await readPayloadFromStaging(nip);
  if (!stagingResult.ok) {
    return stagingResult;
  }

  return postProfile(nip, stagingResult.payload);
}

function createCheckpointSaver(checkpoint) {
  let writeQueue = Promise.resolve();

  return async function flushCheckpoint() {
    writeQueue = writeQueue.then(() => saveCheckpoint(checkpoint));
    return writeQueue;
  };
}

async function processInBatches(nips, checkpoint) {
  let successCount = 0;
  let skipCount = 0;
  let failCount = 0;
  let processedCount = 0;
  let nextIndex = 0;
  const flushCheckpoint = createCheckpointSaver(checkpoint);

  async function handleResult(nip, result) {
    if (result.status === "fulfilled" && result.value?.ok) {
      successCount += 1;
      checkpoint.records[nip] = buildCheckpointEntry("success", result.value.detail);
      logger.info(`[POST] ${nip}: ${result.value.detail}`);
    } else if (result.status === "fulfilled") {
      const outcome = result.value;
      const isSkip =
        (typeof outcome.reason === "string" && outcome.reason.startsWith("staging_")) ||
        (typeof outcome.reason === "string" && outcome.reason.startsWith("invalid_"));

      checkpoint.records[nip] = buildCheckpointEntry(
        isSkip ? "skipped" : "failed",
        outcome.detail,
      );

      if (isSkip) {
        skipCount += 1;
        logger.warn(`[SKIP] ${nip}: ${outcome.detail}`);
      } else {
        failCount += 1;
        logger.error(`[FAIL] ${nip}: ${outcome.detail}`);
      }
    } else {
      failCount += 1;
      checkpoint.records[nip] = buildCheckpointEntry(
        "failed",
        result.reason?.message || "unknown processing error",
      );
      logger.error(
        `[FAIL] ${nip}: ${result.reason?.message || "unknown processing error"}`,
      );
    }

    processedCount += 1;
    await flushCheckpoint();
    logger.info(`[PROGRESS] ${processedCount}/${nips.length} records processed`);
  }

  async function worker() {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= nips.length) {
        return;
      }

      const nip = nips[currentIndex];
      const result = await Promise.allSettled([processNip(nip)]);
      await handleResult(nip, result[0]);
    }
  }

  const workerCount = Math.max(1, Math.min(CONCURRENCY, nips.length));
  await Promise.all(Array.from({ length: workerCount }, () => worker()));

  return { successCount, skipCount, failCount };
}

async function main() {
  if (!ACCESS_TOKEN) {
    logger.error("[CONFIG] EXPORT_ACCESS_TOKEN is required");
    process.exitCode = 1;
    return;
  }

  await ensureCheckpointDir();
  const checkpoint = await loadCheckpoint();
  const nips = getMasterNips();

  if (nips.length === 0) {
    logger.warn("[EXPORT] No NIPs found in ms_employee.json");
    return;
  }

  const queue = getPendingNips(nips, checkpoint);

  logger.info(`[EXPORT] Endpoint: ${EXPORT_URL}`);
  logger.info(`[EXPORT] Total NIPs in master: ${nips.length}`);
  logger.info(`[EXPORT] Already completed from checkpoint: ${nips.length - queue.length}`);
  logger.info(`[EXPORT] Remaining NIPs to process: ${queue.length}`);
  logger.info(`[EXPORT] Concurrency: ${CONCURRENCY}`);

  if (queue.length === 0) {
    logger.info("[DONE] No remaining NIPs to process");
    return;
  }

  const { successCount, skipCount, failCount } = await processInBatches(queue, checkpoint);
  logger.info(`[DONE] Success: ${successCount}`);
  logger.info(`[DONE] Skipped: ${skipCount}`);
  logger.info(`[DONE] Failed: ${failCount}`);
  logger.info(`[DONE] Checkpoint file: ${CHECKPOINT_FILE}`);
}

main().catch((error) => {
  logger.error(`[FATAL] export_profile failed: ${error.message}`);
  process.exitCode = 1;
});
