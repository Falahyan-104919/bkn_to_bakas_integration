#!/usr/bin/env node

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const { PrismaClient } = require("@prisma/client");
const logger = require("./logger");

const prisma = new PrismaClient();

const STAGING_DATA_DIR = path.join(__dirname, "..", "staging_data");
const DEFAULT_DATASET_FILENAME = "1-final.json";
const SUPERADMIN_ID = 1;

const BKN_DOC_ID_TO_FILE_KEY = {
  872: "skJabatan",
  873: "spPelantikan",
};

const LOCAL_FILE_KEY_MAPPING = {
  skJabatan: { field: "trx_jabatan_file_id" },
  spPelantikan: { field: "trx_jabatan_file_spp" },
  baJabatan: { field: "trx_jabatan_file_ba" },
};

const MANAGED_FILE_KEYS = new Set(Object.values(BKN_DOC_ID_TO_FILE_KEY));

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
    deleteFiles: false,
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
      case "--delete-files":
        options.deleteFiles = true;
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

  return options;
}

function printHelp() {
  const lines = [
    "Usage: node script/cleanup_jabatan_files.js [options] [NIP ...]",
    "",
    "Options:",
    "  --dataset <path>   Path to merged dataset JSON (defaults to staging_data/1-final.json if present).",
    "  --commit           Apply changes. Without this flag the script runs in dry-run mode.",
    "  --delete-files     Remove physical PDFs after unlinking (only with --commit).",
    "  --help             Show this message.",
    "",
    "You can optionally supply one or more NIP arguments to restrict processing.",
    "The script only unlinks file references for records that have no mapped document path in the dataset.",
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

function groupRecordsByNip(records, nipFilterSet) {
  const groups = new Map();

  for (const record of records) {
    if (!record || typeof record !== "object") continue;

    const nip = normalizeNip(resolveRecordNip(record));
    if (!nip) {
      logger.warn(
        `[DATASET] Record ${record.id || "<no-id>"} missing NIP. Skipping.`,
      );
      continue;
    }

    if (nipFilterSet && !nipFilterSet.has(nip)) continue;

    if (!groups.has(nip)) {
      groups.set(nip, []);
    }
    groups.get(nip).push(record);
  }

  return groups;
}

function determinePresentFileKeys(record) {
  const presentKeys = new Set();
  if (!record.path || typeof record.path !== "object") {
    return presentKeys;
  }

  for (const [docKey, fileInfo] of Object.entries(record.path)) {
    const fileKeyName = BKN_DOC_ID_TO_FILE_KEY[docKey];
    if (!fileKeyName) {
      if (typeof logger.debug === "function") {
        logger.debug(
          `[PATH] Record ${record.id} has unmapped doc key ${docKey}; skipping.`,
        );
      }
      continue;
    }
    if (!fileInfo || typeof fileInfo.dok_uri !== "string") continue;
    presentKeys.add(fileKeyName);
  }

  return presentKeys;
}

async function processRecordCleanup(options, nip, record, stats) {
  const parsedTmtJabatan = parseDate(record.tmtJabatan);
  if (!parsedTmtJabatan) {
    logger.warn(
      `[SKIP] Record ${record.id} (NIP ${nip}) has invalid TMT ${record.tmtJabatan}.`,
    );
    stats.invalidTmt += 1;
    return;
  }

  const employee = await prisma.ms_employee.findFirst({
    where: {
      employee_nip: record.nipBaru,
      employee_status: { notIn: [0] },
    },
    select: { employee_id: true },
  });
  if (!employee) {
    logger.warn(
      `[SKIP] Employee with NIP ${record.nipBaru} not found for record ${record.id}.`,
    );
    stats.employeeMissing += 1;
    return;
  }

  const jabatanRecord = await prisma.trx_jabatan.findFirst({
    where: {
      trx_jabatan_employee_id: employee.employee_id,
      trx_jabatan_tmt: parsedTmtJabatan,
    },
    select: {
      trx_jabatan_id: true,
      trx_jabatan_file_id: true,
      trx_jabatan_file_spp: true,
      trx_jabatan_file_ba: true,
    },
  });

  if (!jabatanRecord) {
    logger.warn(
      `[SKIP] Jabatan row not found for NIP ${nip} / TMT ${record.tmtJabatan} (record ${record.id}).`,
    );
    stats.jabatanMissing += 1;
    return;
  }

  const hasPathEntries =
    record.path &&
    typeof record.path === "object" &&
    Object.keys(record.path).length > 0;

  const presentFileKeys = hasPathEntries
    ? determinePresentFileKeys(record)
    : new Set();

  if (hasPathEntries && presentFileKeys.size === 0) {
    stats.pathUnmapped += 1;
    logger.info(
      `[SKIP] Record ${record.id} (NIP ${nip}) has document path entries but none map to known file keys; leaving links untouched.`,
    );
    return;
  }

  const unlinkActions = [];

  for (const [fileKey, mapping] of Object.entries(LOCAL_FILE_KEY_MAPPING)) {
    if (!MANAGED_FILE_KEYS.has(fileKey)) continue;
    const columnName = mapping.field;
    const currentFileId = jabatanRecord[columnName];
    if (!currentFileId) continue;

    if (presentFileKeys.has(fileKey)) {
      continue;
    }

    unlinkActions.push({ fileKey, columnName, fileId: currentFileId });
  }

  if (unlinkActions.length === 0) {
    stats.unchanged += 1;
    return;
  }

  stats.toUnlink += unlinkActions.length;

  if (options.dryRun) {
    logger.info(
      `[DRY-RUN] Would unlink ${unlinkActions
        .map((action) => `${action.fileKey}(${action.columnName})`)
        .join(", ")} for NIP ${nip} / record ${record.id}.`,
    );
    return;
  }

  await prisma.$transaction(async (tx) => {
    const jabatanUpdateData = {};
    const fileUpdates = [];

    for (const action of unlinkActions) {
      jabatanUpdateData[action.columnName] = null;
      fileUpdates.push(action);
    }

    await tx.trx_jabatan.update({
      where: { trx_jabatan_id: jabatanRecord.trx_jabatan_id },
      data: jabatanUpdateData,
    });

    for (const action of fileUpdates) {
      const fileRecord = await tx.trx_employee_file.findUnique({
        where: { file_id: action.fileId },
        select: { file_id: true, file_path: true, file_status: true },
      });

      if (!fileRecord) {
        logger.warn(
          `[WARN] File record ${action.fileId} missing while unlinking ${action.fileKey} for NIP ${nip}.`,
        );
        continue;
      }

      const stillUsed = await tx.trx_jabatan.count({
        where: {
          trx_jabatan_id: { not: jabatanRecord.trx_jabatan_id },
          OR: [
            { trx_jabatan_file_id: action.fileId },
            { trx_jabatan_file_spp: action.fileId },
            { trx_jabatan_file_ba: action.fileId },
          ],
        },
      });

      if (stillUsed > 0) {
        logger.info(
          `[INFO] File ${action.fileId} still referenced elsewhere; leaving file record active.`,
        );
        continue;
      }

      await tx.trx_employee_file.update({
        where: { file_id: action.fileId },
        data: {
          file_status: 0,
          file_update_by: SUPERADMIN_ID,
          file_update_date: new Date(),
        },
      });

      stats.filesDisabled += 1;

      if (options.deleteFiles && fileRecord.file_path) {
        try {
          await fsp.unlink(fileRecord.file_path);
          logger.info(`[FILE] Deleted ${fileRecord.file_path}`);
          stats.filesDeleted += 1;
        } catch (err) {
          logger.warn(
            `[FILE] Failed to delete ${fileRecord.file_path}: ${err.message}`,
          );
          stats.fileDeleteFailed += 1;
        }
      } else if (fileRecord.file_path) {
        logger.info(`[FILE] File marked inactive: ${fileRecord.file_path}`);
      }
    }
  });

  logger.info(
    `[CLEANUP] Unlinked ${unlinkActions
      .map((action) => `${action.fileKey}(${action.columnName})`)
      .join(", ")} for NIP ${nip} / record ${record.id}.`,
  );
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

  if (!options.dryRun && !options.datasetPath) {
    logger.error("[ARGS] Dataset path is required when committing changes.");
    process.exitCode = 1;
    return;
  }

  logger.info(
    `--- Starting Jabatan File Cleanup (dry-run=${options.dryRun ? "yes" : "no"}) ---`,
  );

  const datasetRecords = await loadDatasetRecords(options.datasetPath);
  logger.info(
    `[DATASET] Loaded ${datasetRecords.length} record(s) from ${options.datasetPath}`,
  );

  const nipFilter =
    options.nips.length > 0
      ? new Set(options.nips.map((nip) => nip.trim()).filter(Boolean))
      : null;

  const grouped = groupRecordsByNip(datasetRecords, nipFilter);
  if (grouped.size === 0) {
    logger.warn("[DATASET] No records matched the provided filters.");
    return;
  }

  const stats = {
    totalRecords: 0,
    toUnlink: 0,
    unchanged: 0,
    invalidTmt: 0,
    employeeMissing: 0,
    jabatanMissing: 0,
    pathUnmapped: 0,
    filesDisabled: 0,
    filesDeleted: 0,
    fileDeleteFailed: 0,
  };

  for (const [nip, records] of grouped.entries()) {
    logger.info(
      `[PROCESS] NIP ${nip} (${records.length} record${records.length === 1 ? "" : "s"})`,
    );

    for (const record of records) {
      stats.totalRecords += 1;
      await processRecordCleanup(options, nip, record, stats);
    }
  }

  logger.info("--- Cleanup Summary ---");
  logger.info(`Records inspected: ${stats.totalRecords}`);
  logger.info(`Record entries unchanged: ${stats.unchanged}`);
  logger.info(`Unlink operations ${options.dryRun ? "(would run)" : "executed"}: ${stats.toUnlink}`);
  logger.info(`Employees missing: ${stats.employeeMissing}`);
  logger.info(`Jabatan rows missing: ${stats.jabatanMissing}`);
  logger.info(`Invalid TMT skipped: ${stats.invalidTmt}`);
  logger.info(
    `Skipped records with unmapped doc path entries: ${stats.pathUnmapped}`,
  );
  if (!options.dryRun) {
    logger.info(`File records disabled: ${stats.filesDisabled}`);
    if (options.deleteFiles) {
      logger.info(`Physical files deleted: ${stats.filesDeleted}`);
      logger.info(`File deletes failed: ${stats.fileDeleteFailed}`);
    }
  }

  logger.info(
    `--- Cleanup Finished (dry-run=${options.dryRun ? "yes" : "no"}) ---`,
  );
}

if (require.main === module) {
  main()
    .catch((err) => {
      logger.error(`[FATAL] Cleanup failed: ${err.message}`);
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
