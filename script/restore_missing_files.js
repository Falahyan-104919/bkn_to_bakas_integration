#!/usr/bin/env node

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const axios = require("axios");
const { URLSearchParams } = require("url");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config();

const logger = require("./logger");

const prisma = new PrismaClient();

const STAGING_DATA_DIR = path.join(__dirname, "..", "staging_data");
const DEFAULT_DATASET_FILENAME = "1-final.json";
const DOWNLOAD_PATH = "/download-dok";
const SUPERADMIN_ID = 1;

const API_BASE_URL = process.env.API_BASE_URL;
const TOKEN_URL = process.env.TOKEN_URL;
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const STATIC_AUTH_TOKEN = process.env.STATIC_AUTH_TOKEN;

const COLUMN_TO_DOC_KEY = {
  trx_jabatan_file_id: "skJabatan",
  trx_jabatan_file_spp: "spPelantikan",
  trx_jabatan_file_ba: "baJabatan",
};

const DOC_KEY_TO_BKN_ID = {
  skJabatan: "872",
  spPelantikan: "873",
  baJabatan: "874",
};

function sanitizeString(str) {
  if (typeof str !== "string") return str;
  return str.replace(/[\uFFFD]/g, "");
}

function parseDate(dateString) {
  if (!dateString || typeof dateString !== "string") return null;
  const [dayString, monthString, yearString] = dateString.split("-");
  if (!dayString || !monthString || !yearString) return null;
  const day = Number.parseInt(dayString, 10);
  const month = Number.parseInt(monthString, 10);
  const year = Number.parseInt(yearString, 10);
  if (
    !Number.isInteger(day) ||
    !Number.isInteger(month) ||
    !Number.isInteger(year) ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31
  ) {
    return null;
  }
  const parsedDate = new Date(year, month - 1, day);
  if (
    Number.isNaN(parsedDate.getTime()) ||
    parsedDate.getFullYear() !== year ||
    parsedDate.getMonth() !== month - 1 ||
    parsedDate.getDate() !== day
  ) {
    return null;
  }
  return parsedDate;
}

function formatDateDDMMYYYY(date) {
  const dd = String(date.getDate()).padStart(2, "0");
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const yyyy = date.getFullYear();
  return `${dd}-${mm}-${yyyy}`;
}

function normalizeNip(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }
  if (typeof value === "number") {
    return String(value);
  }
  return null;
}

function resolveRecordNip(record) {
  return (
    record?.nipBaru ??
    record?.nip ??
    record?.employee_nip ??
    record?.employeeNip ??
    record?.nipbaru ??
    null
  );
}

function parseCliArgs(argv) {
  const options = {
    datasetPath: null,
    nips: [],
    dryRun: true,
    help: false,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    switch (arg) {
      case "--dataset":
        if (i + 1 >= argv.length) {
          throw new Error("--dataset requires a path argument.");
        }
        options.datasetPath = argv[++i];
        break;
      case "--commit":
        options.dryRun = false;
        break;
      case "--help":
      case "-h":
        options.help = true;
        break;
      default:
        if (arg.startsWith("--")) {
          throw new Error(`Unknown option: ${arg}`);
        }
        if (arg.trim().length > 0) {
          options.nips.push(arg.trim());
        }
        break;
    }
  }

  if (!options.datasetPath) {
    const defaultCandidate = path.join(STAGING_DATA_DIR, DEFAULT_DATASET_FILENAME);
    if (fs.existsSync(defaultCandidate)) {
      options.datasetPath = defaultCandidate;
      logger.info(`[CONFIG] Using default dataset ${DEFAULT_DATASET_FILENAME}.`);
    } else {
      throw new Error(
        "Dataset path is required. Provide --dataset <path> or place 1-final.json in staging_data.",
      );
    }
  } else {
    options.datasetPath = path.resolve(process.cwd(), options.datasetPath);
  }

  if (!API_BASE_URL || !TOKEN_URL || !CLIENT_ID || !CLIENT_SECRET || !STATIC_AUTH_TOKEN) {
    throw new Error("Missing required API credentials in environment variables.");
  }

  return options;
}

function printHelp() {
  const lines = [
    "Usage: node script/restore_missing_files.js [options] [NIP ...]",
    "",
    "Options:",
    "  --dataset <path>   Path to merged dataset JSON (defaults to staging_data/1-final.json if present).",
    "  --commit           Actually download and restore files (default: dry-run).",
    "  --help             Show this message.",
    "",
    "Specify one or more NIPs to limit recovery. Dry-run lists missing files without downloading.",
  ];
  lines.forEach((line) => logger.info(line));
}

async function loadDatasetRecords(datasetPath) {
  const raw = await fsp.readFile(datasetPath, "utf-8");
  const sanitized = sanitizeString(raw) ?? raw;
  const parsed = JSON.parse(sanitized);

  if (Array.isArray(parsed)) {
    return parsed;
  }

  if (parsed && Array.isArray(parsed.data)) {
    return parsed.data;
  }

  throw new Error(
    `${datasetPath} does not contain an array or an object with a 'data' array.`,
  );
}

function buildDatasetIndex(records) {
  const index = new Map(); // key: `${nip}__${tmt}` lower-case

  for (const record of records) {
    if (!record || typeof record !== "object") continue;
    const nip = normalizeNip(resolveRecordNip(record));
    if (!nip) continue;
    const tmt = record.tmtJabatan;
    if (!tmt) continue;

    const key = `${nip}__${tmt}`;
    if (!index.has(key)) {
      index.set(key, []);
    }
    index.get(key).push(record);
  }

  return index;
}

async function fetchDynamicToken() {
  logger.info("[AUTH] Requesting dynamic token");
  const body = new URLSearchParams();
  body.append("grant_type", "client_credentials");
  body.append("client_id", CLIENT_ID);
  body.append("client_secret", CLIENT_SECRET);

  const response = await axios.post(TOKEN_URL, body, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });
  logger.info("[AUTH] Dynamic token acquired");
  return response.data.access_token;
}

async function withTokenRetry(makeRequest, tokenRef, context) {
  try {
    return await makeRequest(tokenRef.current);
  } catch (error) {
    if (error.response && error.response.status === 401) {
      logger.warn(`[AUTH] Token expired during ${context}, refreshing…`);
      tokenRef.current = await fetchDynamicToken();
      return makeRequest(tokenRef.current);
    }
    throw error;
  }
}

async function ensureDirectoryForFile(filePath) {
  const dir = path.dirname(filePath);
  await fsp.mkdir(dir, { recursive: true });
}

async function downloadFile({ filePath, downloadUri, tokenRef }) {
  const encoded = encodeURIComponent(downloadUri);
  const url = `${API_BASE_URL}${DOWNLOAD_PATH}?filePath=${encoded}`;

  const response = await withTokenRetry(
    (token) =>
      axios.get(url, {
        responseType: "stream",
        headers: {
          accept: "*/*",
          Authorization: `Bearer ${token}`,
          Auth: `Bearer ${STATIC_AUTH_TOKEN}`,
        },
      }),
    tokenRef,
    `download ${downloadUri}`,
  );

  const tempPath = `${filePath}.tmp`;
  await ensureDirectoryForFile(filePath);

  await new Promise((resolve, reject) => {
    const writer = fs.createWriteStream(tempPath);
    response.data.pipe(writer);
    writer.on("finish", resolve);
    writer.on("error", reject);
  });

  const stats = await fsp.stat(tempPath);
  if (stats.size === 0) {
    await fsp.unlink(tempPath);
    throw new Error("Downloaded file is empty");
  }

  if (fs.existsSync(filePath)) {
    await fsp.unlink(filePath);
  }

  await fsp.rename(tempPath, filePath);
  return stats.size;
}

async function findDatasetRecord(datasetIndex, nip, tmt) {
  const key = `${nip}__${tmt}`;
  return datasetIndex.get(key) || [];
}

async function gatherJabatanFileLinks(nipFilter) {
  const rows = await prisma.trx_jabatan.findMany({
    where: {
      OR: [
        { trx_jabatan_file_id: { not: null } },
        { trx_jabatan_file_spp: { not: null } },
        { trx_jabatan_file_ba: { not: null } },
      ],
      ...(nipFilter
        ? {
            ms_employee: {
              employee_nip: { in: Array.from(nipFilter) },
            },
          }
        : {}),
    },
    select: {
      trx_jabatan_id: true,
      trx_jabatan_tmt: true,
      trx_jabatan_file_id: true,
      trx_jabatan_file_spp: true,
      trx_jabatan_file_ba: true,
      ms_employee: {
        select: {
          employee_nip: true,
        },
      },
    },
  });

  return rows;
}

async function gatherFileRecords(fileIds) {
  if (fileIds.length === 0) return new Map();

  const files = await prisma.trx_employee_file.findMany({
    where: { file_id: { in: fileIds } },
    select: {
      file_id: true,
      file_path: true,
      file_status: true,
    },
  });

  return new Map(files.map((file) => [file.file_id, file]));
}

async function main() {
  let options;
  try {
    options = parseCliArgs(process.argv.slice(2));
  } catch (err) {
    logger.error(`[ARGS] ${err.message}`);
    printHelp();
    process.exitCode = 1;
    return;
  }

  if (options.help) {
    printHelp();
    return;
  }

  logger.info(
    `--- Starting Restore Missing Files (dry-run=${options.dryRun ? "yes" : "no"}) ---`,
  );

  const datasetRecords = await loadDatasetRecords(options.datasetPath);
  logger.info(
    `[DATASET] Loaded ${datasetRecords.length} record(s) from ${options.datasetPath}`,
  );

  const datasetIndex = buildDatasetIndex(datasetRecords);

  const nipFilter =
    options.nips.length > 0
      ? new Set(options.nips.map((nip) => nip.trim()).filter(Boolean))
      : null;

  const jabatanRows = await gatherJabatanFileLinks(nipFilter);
  logger.info(
    `[DB] Found ${jabatanRows.length} jabatan row(s) with linked files to inspect.`,
  );

  const fileIds = new Set();
  for (const row of jabatanRows) {
    for (const column of Object.keys(COLUMN_TO_DOC_KEY)) {
      const value = row[column];
      if (value) fileIds.add(value);
    }
  }

  const fileMap = await gatherFileRecords(Array.from(fileIds));

  const tokenRef = { current: null };

  const stats = {
    totalLinks: 0,
    missingFiles: 0,
    restored: 0,
    skippedDatasetMissing: 0,
    skippedDocMissing: 0,
    errors: 0,
    reactivated: 0,
  };

  if (!options.dryRun) {
    tokenRef.current = await fetchDynamicToken();
  }

  for (const row of jabatanRows) {
    const nip = row.ms_employee.employee_nip;
    const tmtDate = row.trx_jabatan_tmt;
    const tmtString = formatDateDDMMYYYY(tmtDate);
    const datasetCandidates = await findDatasetRecord(datasetIndex, nip, tmtString);

    if (!datasetCandidates || datasetCandidates.length === 0) {
      logger.warn(
        `[WARN] Dataset missing entry for NIP ${nip} / TMT ${tmtString}. Skipping row ${row.trx_jabatan_id}.`,
      );
      stats.skippedDatasetMissing += 1;
      continue;
    }

    const datasetRecord = datasetCandidates[0];

    for (const [columnName, docKey] of Object.entries(COLUMN_TO_DOC_KEY)) {
      const fileId = row[columnName];
      if (!fileId) continue;
      stats.totalLinks += 1;

      const fileRecord = fileMap.get(fileId);
      if (!fileRecord) {
        logger.warn(
          `[WARN] File record ${fileId} missing for jabatan ${row.trx_jabatan_id}; skipping.`,
        );
        stats.errors += 1;
        continue;
      }

      if (!fileRecord.file_path) {
        logger.warn(
          `[WARN] File ${fileId} has no path stored; skipping.`,
        );
        stats.errors += 1;
        continue;
      }

      if (fs.existsSync(fileRecord.file_path)) {
        continue;
      }

      stats.missingFiles += 1;

      const bknDocId = DOC_KEY_TO_BKN_ID[docKey];
      const pathEntry =
        datasetRecord?.path?.[bknDocId] ||
        datasetRecord?.path?.[Number(bknDocId)];

      if (!datasetRecord.path || !pathEntry || !pathEntry.dok_uri) {
        logger.warn(
          `[WARN] Dataset record ${datasetRecord.id} (NIP ${nip} / TMT ${tmtString}) has no doc uri for ${docKey}; skipping recovery.`,
        );
        stats.skippedDocMissing += 1;
        continue;
      }

      if (options.dryRun) {
        logger.info(
          `[DRY-RUN] Missing file ${fileId} (${docKey}) for NIP ${nip} / TMT ${tmtString} would be downloaded from ${pathEntry.dok_uri}`,
        );
        continue;
      }

      try {
        const size = await downloadFile({
          filePath: fileRecord.file_path,
          downloadUri: pathEntry.dok_uri,
          tokenRef,
        });
        logger.info(
          `[RESTORE] Restored file ${fileId} (${docKey}) for NIP ${nip}; ${size} bytes written.`,
        );
        stats.restored += 1;

        if (fileRecord.file_status === 0) {
          await prisma.trx_employee_file.update({
            where: { file_id: fileId },
            data: {
              file_status: 1,
              file_update_by: SUPERADMIN_ID,
              file_update_date: new Date(),
            },
          });
          stats.reactivated += 1;
        }
      } catch (err) {
        stats.errors += 1;
        logger.error(
          `[FAIL] Failed to restore file ${fileId} (${docKey}) for NIP ${nip}: ${err.message}`,
        );
      }
    }
  }

  logger.info("--- Restore Summary ---");
  logger.info(`Total linked files inspected: ${stats.totalLinks}`);
  logger.info(`Missing file slots: ${stats.missingFiles}`);
  logger.info(`Dataset missing entries: ${stats.skippedDatasetMissing}`);
  logger.info(`Dataset missing doc uris: ${stats.skippedDocMissing}`);
  if (!options.dryRun) {
    logger.info(`Files restored: ${stats.restored}`);
    logger.info(`File statuses reactivated: ${stats.reactivated}`);
  }
  logger.info(`Errors encountered: ${stats.errors}`);

  logger.info(
    `--- Restore Finished (dry-run=${options.dryRun ? "yes" : "no"}) ---`,
  );
}

if (require.main === module) {
  main()
    .catch((err) => {
      logger.error(`[FATAL] Restore failed: ${err.message}`);
      logger.error(err.stack);
      process.exit(1);
    })
    .finally(async () => {
      await prisma.$disconnect();
      logger.info("--- Database disconnected ---");
    });
}

module.exports = {
  main,
};
