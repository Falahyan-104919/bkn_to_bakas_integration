#!/usr/bin/env node

require("dotenv").config();

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const { PrismaClient, Prisma } = require("@prisma/client");

const logger = require("./logger");

const prisma = new PrismaClient();

const STAGING_DATA_DIR = path.join(__dirname, "..", "staging_data");
const DEFAULT_DATASET_FILENAME = "1-final.json";
const SUPERADMIN_ID = 1;

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
    extraNipValues: [],
    extraNipFiles: [],
    positionalNips: [],
    includeAll: false,
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
      case "--extra-nips":
      case "--nips":
        if (i + 1 >= argv.length) {
          throw new Error(`${arg} requires a comma/space separated list.`);
        }
        options.extraNipValues.push(argv[++i]);
        break;
      case "--extra-nips-file":
      case "--nips-file":
        if (i + 1 >= argv.length) {
          throw new Error(`${arg} requires a file path.`);
        }
        options.extraNipFiles.push(argv[++i]);
        break;
      case "--include-all":
        options.includeAll = true;
        break;
      case "--commit":
        options.dryRun = false;
        break;
      case "--dry-run":
        options.dryRun = true;
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
          options.positionalNips.push(arg.trim());
        }
        break;
    }
  }

  return options;
}

function printHelp() {
  const lines = [
    "Usage: node script/dedupe_jabatan.js [options] [NIP ...]",
    "",
    "Options:",
    "  --dataset <path>         Path to merged dataset JSON (defaults to staging_data/1-final.json).",
    "  --extra-nips \"A,B\"       Limit to specific NIPs (comma/space separated).",
    "  --extra-nips-file <path> Load NIPs from a file.",
    "  --include-all            Check every NIP (ignore filters).",
    "  --dry-run                Report duplicates without modifying data (default).",
    "  --commit                 Remove redundant rows and disable orphaned files.",
    "  --help                   Show this message.",
    "",
    "The script uses the dataset to decide which jabatan row per NIP/TMT should remain.",
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
  const index = new Map();

  for (const record of records) {
    if (!record || typeof record !== "object") continue;
    const nip = normalizeNip(resolveRecordNip(record));
    if (!nip) continue;
    const key = `${nip}__${record.tmtJabatan}`;
    if (!index.has(key)) {
      index.set(key, []);
    }
    index.get(key).push(record);
  }

  return index;
}

async function resolveNipFilter(options) {
  const nipSet = new Set();

  const addValues = (chunk) => {
    if (!chunk) return;
    const nips = chunk
      .split(/[\s,]+/)
      .map((value) => value.trim())
      .filter(Boolean);
    nips.forEach((nip) => nipSet.add(nip));
  };

  options.positionalNips.forEach(addValues);
  options.extraNipValues.forEach(addValues);

  for (const filePath of options.extraNipFiles) {
    const absolute = path.resolve(process.cwd(), filePath);
    let contents;
    try {
      contents = await fsp.readFile(absolute, "utf-8");
    } catch (err) {
      throw new Error(`Unable to read NIP list file "${filePath}": ${err.message}`);
    }
    addValues(contents.replace(/\r/g, "\n"));
  }

  return options.includeAll ? null : new Set(nipSet);
}

async function findDuplicateGroups(nipFilter) {
  const results = await prisma.$queryRaw(
    Prisma.sql`
      SELECT
        e.employee_nip AS nip,
        j.trx_jabatan_employee_id AS employee_id,
        j.trx_jabatan_tmt AS tmt,
        COUNT(*) AS row_count
      FROM trx_jabatan j
      JOIN ms_employee e ON e.employee_id = j.trx_jabatan_employee_id
      ${nipFilter && nipFilter.size > 0
        ? Prisma.sql`WHERE e.employee_nip IN (${Prisma.join(Array.from(nipFilter))})`
        : Prisma.empty}
      GROUP BY e.employee_nip, j.trx_jabatan_tmt
      HAVING COUNT(*) > 1
    `,
  );

  return results.map((row) => ({
    nip: row.nip,
    employeeId: row.employee_id,
    tmt: row.tmt,
  }));
}

async function loadJabatanRows(employeeId, tmt) {
  const parsedTmt = new Date(tmt);
  return prisma.trx_jabatan.findMany({
    where: {
      trx_jabatan_employee_id: employeeId,
      trx_jabatan_tmt: parsedTmt,
    },
    select: {
      trx_jabatan_id: true,
      trx_jabatan_employee_id: true,
      trx_jabatan_tmt: true,
      trx_jabatan_bkn_id: true,
      trx_jabatan_file_id: true,
      trx_jabatan_file_spp: true,
      trx_jabatan_file_ba: true,
      trx_jabatan_update_date: true,
      trx_jabatan_create_date: true,
    },
    orderBy: { trx_jabatan_id: "asc" },
  });
}

function scoreRow(row, datasetRecords) {
  const datasetMatch = datasetRecords.some(
    (record) => record.id && record.id === row.trx_jabatan_bkn_id,
  );
  const fileCount = [
    row.trx_jabatan_file_id,
    row.trx_jabatan_file_spp,
    row.trx_jabatan_file_ba,
  ].filter(Boolean).length;
  const updateTime =
    row.trx_jabatan_update_date ||
    row.trx_jabatan_create_date ||
    new Date(0);

  return {
    datasetMatch,
    fileCount,
    updateTime,
    jabatanId: row.trx_jabatan_id,
  };
}

function chooseKeepRow(rows, datasetRecords) {
  let bestRow = rows[0];
  let bestScore = scoreRow(bestRow, datasetRecords);

  for (let i = 1; i < rows.length; i++) {
    const candidate = rows[i];
    const candidateScore = scoreRow(candidate, datasetRecords);

    if (candidateScore.datasetMatch && !bestScore.datasetMatch) {
      bestRow = candidate;
      bestScore = candidateScore;
      continue;
    }

    if (candidateScore.datasetMatch === bestScore.datasetMatch) {
      if (candidateScore.fileCount > bestScore.fileCount) {
        bestRow = candidate;
        bestScore = candidateScore;
        continue;
      }

      if (
        candidateScore.fileCount === bestScore.fileCount &&
        candidateScore.updateTime > bestScore.updateTime
      ) {
        bestRow = candidate;
        bestScore = candidateScore;
        continue;
      }

      if (
        candidateScore.fileCount === bestScore.fileCount &&
        candidateScore.updateTime.getTime() === bestScore.updateTime.getTime() &&
        candidateScore.jabatanId > bestScore.jabatanId
      ) {
        bestRow = candidate;
        bestScore = candidateScore;
      }
    }
  }

  return bestRow;
}

async function mergeFileLinks(tx, keepRow, redundantRow) {
  const updates = {};
  if (!keepRow.trx_jabatan_file_id && redundantRow.trx_jabatan_file_id) {
    updates.trx_jabatan_file_id = redundantRow.trx_jabatan_file_id;
  }
  if (!keepRow.trx_jabatan_file_spp && redundantRow.trx_jabatan_file_spp) {
    updates.trx_jabatan_file_spp = redundantRow.trx_jabatan_file_spp;
  }
  if (!keepRow.trx_jabatan_file_ba && redundantRow.trx_jabatan_file_ba) {
    updates.trx_jabatan_file_ba = redundantRow.trx_jabatan_file_ba;
  }

  if (Object.keys(updates).length === 0) {
    return keepRow;
  }

  await tx.trx_jabatan.update({
    where: { trx_jabatan_id: keepRow.trx_jabatan_id },
    data: updates,
  });

  return { ...keepRow, ...updates };
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

  let datasetPath = options.datasetPath;
  if (!datasetPath) {
    const defaultCandidate = path.join(STAGING_DATA_DIR, DEFAULT_DATASET_FILENAME);
    if (fs.existsSync(defaultCandidate)) {
      datasetPath = defaultCandidate;
      logger.info(
        `[CONFIG] Using default dataset ${DEFAULT_DATASET_FILENAME}.`,
      );
    } else {
      logger.warn(
        "[CONFIG] No dataset provided and default 1-final.json not found. Deduplicating without dataset reference.",
      );
    }
  } else {
    datasetPath = path.resolve(process.cwd(), datasetPath);
  }

  let datasetIndex = null;
  if (datasetPath) {
    try {
      const datasetRecords = await loadDatasetRecords(datasetPath);
      datasetIndex = buildDatasetIndex(datasetRecords);
      logger.info(
        `[DATASET] Indexed ${datasetIndex.size} record(s) from ${datasetPath}.`,
      );
    } catch (err) {
      logger.error(`[DATASET] Failed to load dataset: ${err.message}`);
      process.exitCode = 1;
      return;
    }
  }

  let nipFilter;
  try {
    nipFilter = await resolveNipFilter(options);
  } catch (err) {
    logger.error(`[ARGS] ${err.message}`);
    process.exitCode = 1;
    return;
  }

  const duplicates = await findDuplicateGroups(nipFilter);
  if (duplicates.length === 0) {
    logger.info("[CHECK] No duplicate jabatan rows found.");
    return;
  }

  logger.info(
    `[CHECK] Found ${duplicates.length} duplicate NIP/TMT group(s). Dry-run=${options.dryRun ? "YES" : "NO"}.`,
  );

  const stats = {
    groups: duplicates.length,
    rowsReviewed: 0,
    rowsDeleted: 0,
    filesDisabled: 0,
    errors: 0,
  };

  for (const group of duplicates) {
    const { nip, employeeId, tmt } = group;
    const tmtDate = new Date(tmt);
    const tmtString = formatDateDDMMYYYY(tmtDate);
    const rows = await loadJabatanRows(employeeId, tmt);
    stats.rowsReviewed += rows.length;

    if (rows.length < 2) continue;

    const datasetRecords =
      datasetIndex?.get(`${nip}__${tmtString}`) ?? [];

    let keepRow = chooseKeepRow(rows, datasetRecords);

    const redundantRows = rows.filter(
      (row) => row.trx_jabatan_id !== keepRow.trx_jabatan_id,
    );

    logger.info(
      `[GROUP] NIP ${nip}, TMT ${tmtString}: keeping ${keepRow.trx_jabatan_id}, removing ${redundantRows
        .map((row) => row.trx_jabatan_id)
        .join(", ")}`,
    );

    if (options.dryRun) {
      continue;
    }

    try {
      await prisma.$transaction(async (tx) => {
        let currentKeepRow = keepRow;
        for (const redundant of redundantRows) {
          currentKeepRow = await mergeFileLinks(tx, currentKeepRow, redundant);

          const fileIds = [
            redundant.trx_jabatan_file_id,
            redundant.trx_jabatan_file_spp,
            redundant.trx_jabatan_file_ba,
          ].filter(Boolean);

          for (const fileId of fileIds) {
            const stillUsed = await tx.trx_jabatan.count({
              where: {
                trx_jabatan_id: { not: redundant.trx_jabatan_id },
                OR: [
                  { trx_jabatan_file_id: fileId },
                  { trx_jabatan_file_spp: fileId },
                  { trx_jabatan_file_ba: fileId },
                ],
              },
            });

            if (stillUsed > 0) {
              continue;
            }

            await tx.trx_employee_file.updateMany({
              where: { file_id: fileId },
              data: {
                file_status: 0,
                file_update_by: SUPERADMIN_ID,
                file_update_date: new Date(),
              },
            });
            stats.filesDisabled += 1;
          }

          await tx.trx_jabatan.delete({
            where: { trx_jabatan_id: redundant.trx_jabatan_id },
          });
          stats.rowsDeleted += 1;
        }
      });
    } catch (err) {
      stats.errors += 1;
      logger.error(
        `[FAIL] Failed processing group NIP ${nip} / TMT ${tmtString}: ${err.message}`,
      );
    }
  }

  logger.info("--- Dedupe Summary ---");
  logger.info(`Duplicate groups: ${stats.groups}`);
  logger.info(`Rows reviewed: ${stats.rowsReviewed}`);
  logger.info(`Rows deleted: ${stats.rowsDeleted}`);
  logger.info(`File rows disabled: ${stats.filesDisabled}`);
  logger.info(`Errors: ${stats.errors}`);
  logger.info(
    `--- Dedupe Finished (dry-run=${options.dryRun ? "yes" : "no"}) ---`,
  );
}

if (require.main === module) {
  main()
    .catch((err) => {
      logger.error(`[FATAL] Dedupe failed: ${err.message}`);
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
