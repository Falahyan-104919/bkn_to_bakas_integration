#!/usr/bin/env node

require("dotenv").config();

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const { PrismaClient, Prisma } = require("@prisma/client");
const logger = require("./logger");

const prisma = new PrismaClient();

const FILE_COLUMNS = [
  { column: "trx_jabatan_file_id", label: "skJabatan", datasetDocs: ["872"] },
  {
    column: "trx_jabatan_file_spp",
    label: "spPelantikan",
    datasetDocs: ["873"],
  },
  {
    column: "trx_jabatan_file_ba",
    label: "baJabatan",
    datasetDocs: ["874"],
  },
];

function parseArgList(value) {
  if (!value) return [];
  return value
    .split(/[\s,]+/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function parseCliArgs(argv) {
  const options = {
    nips: [],
    nipFiles: [],
    includeAll: false,
    checkStatus: true,
    checkFs: true,
    verbose: false,
    datasetPath: null,
    datasetOnly: false,
    help: false,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    switch (arg) {
      case "--nips":
      case "--extra-nips":
        if (i + 1 >= argv.length) {
          throw new Error(`${arg} requires a comma/space separated list.`);
        }
        options.nips.push(argv[++i]);
        break;
      case "--nips-file":
      case "--extra-nips-file":
        if (i + 1 >= argv.length) {
          throw new Error(`${arg} requires a file path.`);
        }
        options.nipFiles.push(argv[++i]);
        break;
      case "--include-all":
        options.includeAll = true;
        break;
      case "--skip-status":
        options.checkStatus = false;
        break;
      case "--skip-fs":
        options.checkFs = false;
        break;
      case "--verbose":
        options.verbose = true;
        break;
      case "--dataset":
        if (i + 1 >= argv.length) {
          throw new Error("--dataset requires a path argument.");
        }
        options.datasetPath = argv[++i];
        break;
      case "--dataset-only":
        options.datasetOnly = true;
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

function printHelp() {
  const lines = [
    "Usage: node script/validate_jabatan_files_db.js [options]",
    "",
    "Options:",
    "  --nips \"A,B\"            Limit validation to specific NIPs (comma/space separated).",
    "  --nips-file <path>      Load NIPs from a file (one per line or comma separated).",
    "  --include-all           Scan all jabatan rows (ignore filters).",
    "  --skip-status           Do not flag file_status != 1 as an error.",
    "  --skip-fs               Skip filesystem existence checks.",
    "  --verbose               Print per-link validation details.",
    "  --dataset <path>        Cross-check dataset documents against DB/filesystem.",
    "  --dataset-only          Only run dataset cross-check (skip DB link scan).",
    "  --help                  Show this message.",
  ];
  lines.forEach((line) => logger.info(line));
}

async function resolveNipFilter(options) {
  if (options.includeAll) return null;

  const nipSet = new Set();
  for (const chunk of options.nips) {
    parseArgList(chunk).forEach((nip) => nipSet.add(nip));
  }

  for (const filePath of options.nipFiles) {
    const absolute = path.resolve(process.cwd(), filePath);
    const contents = await fsp.readFile(absolute, "utf-8");
    parseArgList(contents.replace(/\r/g, "\n")).forEach((nip) =>
      nipSet.add(nip),
    );
  }

  return nipSet.size === 0 ? null : new Set(nipSet);
}

async function fetchJabatanRows(nipFilter) {
  if (nipFilter && nipFilter.size === 0) return [];

  const rows = await prisma.$queryRaw(
    Prisma.sql`
      SELECT
        j.trx_jabatan_id,
        j.trx_jabatan_employee_id,
        j.trx_jabatan_tmt,
        j.trx_jabatan_update_date,
        j.trx_jabatan_create_date,
        j.trx_jabatan_nomor_sk,
        j.trx_jabatan_bkn_id,
        j.trx_jabatan_file_id,
        j.trx_jabatan_file_spp,
        j.trx_jabatan_file_ba,
        e.employee_nip
      FROM trx_jabatan j
      JOIN ms_employee e ON e.employee_id = j.trx_jabatan_employee_id
      ${nipFilter && nipFilter.size > 0
        ? Prisma.sql`WHERE e.employee_nip IN (${Prisma.join(Array.from(nipFilter))})`
        : Prisma.empty}
      ORDER BY e.employee_nip, j.trx_jabatan_tmt, j.trx_jabatan_id
    `,
  );

  return rows.map((row) => ({
    ...row,
    trx_jabatan_tmt: row.trx_jabatan_tmt
      ? new Date(row.trx_jabatan_tmt)
      : null,
    trx_jabatan_update_date: row.trx_jabatan_update_date
      ? new Date(row.trx_jabatan_update_date)
      : null,
    trx_jabatan_create_date: row.trx_jabatan_create_date
      ? new Date(row.trx_jabatan_create_date)
      : null,
  }));
}

async function fetchFileRecords(fileIds) {
  if (fileIds.length === 0) return new Map();

  const files = await prisma.trx_employee_file.findMany({
    where: { file_id: { in: fileIds } },
    select: {
      file_id: true,
      file_name: true,
      file_path: true,
      file_status: true,
      file_update_date: true,
    },
  });

  return new Map(files.map((file) => [file.file_id, file]));
}

function formatDateDDMMYYYY(date) {
  const dd = String(date.getDate()).padStart(2, "0");
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const yyyy = String(date.getFullYear());
  return `${dd}-${mm}-${yyyy}`;
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

async function loadDatasetIndex(datasetPath, nipFilter) {
  const absolute = path.resolve(process.cwd(), datasetPath);
  const raw = await fsp.readFile(absolute, "utf-8");
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error(`Dataset JSON parse error: ${err.message}`);
  }

  const records = Array.isArray(parsed?.data)
    ? parsed.data
    : Array.isArray(parsed)
      ? parsed
      : [];

  const index = new Map();

  for (const record of records) {
    if (!record || typeof record !== "object") continue;
    const nip = resolveRecordNip(record);
    if (!nip || (nipFilter && !nipFilter.has(nip))) continue;
    if (!record.tmtJabatan) continue;
    const key = `${nip}__${record.tmtJabatan}`;
    if (!index.has(key)) {
      index.set(key, []);
    }
    index.get(key).push(record);
  }

  return index;
}

function chooseBestJabatanRow(rows, column) {
  if (!rows || rows.length === 0) return null;
  let best = rows[0];
  for (let i = 1; i < rows.length; i++) {
    const candidate = rows[i];
    const bestHas = Boolean(best[column]);
    const candidateHas = Boolean(candidate[column]);
    if (candidateHas && !bestHas) {
      best = candidate;
      continue;
    }
    if (candidateHas === bestHas) {
      const bestTime =
        best.trx_jabatan_update_date || best.trx_jabatan_create_date || new Date(0);
      const candidateTime =
        candidate.trx_jabatan_update_date ||
        candidate.trx_jabatan_create_date ||
        new Date(0);
      if (candidateTime > bestTime) {
        best = candidate;
        continue;
      }
      if (
        candidateTime.getTime() === bestTime.getTime() &&
        candidate.trx_jabatan_id > best.trx_jabatan_id
      ) {
        best = candidate;
      }
    }
  }
  return best;
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

  if (options.datasetOnly && !options.datasetPath) {
    logger.error("[ARGS] --dataset-only requires --dataset <path>.");
    process.exitCode = 1;
    return;
  }

  let nipFilter;
  try {
    nipFilter = await resolveNipFilter(options);
  } catch (err) {
    logger.error(`[ARGS] ${err.message}`);
    process.exitCode = 1;
    return;
  }

  const jabatanRows = await fetchJabatanRows(nipFilter);
  const fileIds = new Set();
  const jabatanByKey = new Map();

  for (const row of jabatanRows) {
    if (row.trx_jabatan_tmt) {
      const key = `${row.employee_nip}__${formatDateDDMMYYYY(
        row.trx_jabatan_tmt,
      )}`;
      if (!jabatanByKey.has(key)) {
        jabatanByKey.set(key, []);
      }
      jabatanByKey.get(key).push(row);
    }
    FILE_COLUMNS.forEach(({ column }) => {
      if (row[column]) fileIds.add(row[column]);
    });
  }

  const fileMap = await fetchFileRecords(Array.from(fileIds));

  if (!options.datasetOnly) {
    logger.info(
      `--- Starting DB Jabatan/File Validation (filters=${
        nipFilter ? `${nipFilter.size} NIP(s)` : "none"
      }) ---`,
    );

    if (jabatanRows.length === 0) {
      logger.warn("[CHECK] No jabatan rows matched the filters.");
    } else {
      const stats = {
        rows: jabatanRows.length,
        linksChecked: 0,
        missingFileRecord: 0,
        missingFilePath: 0,
        statusIssues: 0,
        ok: 0,
      };

      for (const row of jabatanRows) {
        const nip = row.employee_nip ?? "(unknown)";
        const tmt = row.trx_jabatan_tmt
          ? formatDateDDMMYYYY(row.trx_jabatan_tmt)
          : "(no tmt)";

        for (const { column, label } of FILE_COLUMNS) {
          const fileId = row[column];
          if (!fileId) continue;
          stats.linksChecked += 1;

          const fileRecord = fileMap.get(fileId);
          if (!fileRecord) {
            stats.missingFileRecord += 1;
            logger.error(
              `[MISSING_RECORD] NIP ${nip} / TMT ${tmt} / ${label}: file_id ${fileId} not found in trx_employee_file.`,
            );
            continue;
          }

          const issues = [];

          if (options.checkStatus && fileRecord.file_status !== 1) {
            stats.statusIssues += 1;
            issues.push(`status=${fileRecord.file_status}`);
          }

          if (options.checkFs) {
            if (!fileRecord.file_path) {
              stats.missingFilePath += 1;
              issues.push("path-empty");
            } else if (!fs.existsSync(fileRecord.file_path)) {
              stats.missingFilePath += 1;
              issues.push(`path-missing:${fileRecord.file_path}`);
            }
          }

          if (issues.length > 0) {
            logger.warn(
              `[WARN] NIP ${nip} / TMT ${tmt} / ${label} (file_id ${fileId}): ${issues.join(", ")}`,
            );
          } else {
            stats.ok += 1;
            if (options.verbose) {
              logger.info(
                `[OK] NIP ${nip} / TMT ${tmt} / ${label} -> ${fileRecord.file_path}`,
              );
            }
          }
        }
      }

      logger.info("--- DB Link Validation Summary ---");
      logger.info(`Jabatan rows scanned: ${stats.rows}`);
      logger.info(`Linked files checked: ${stats.linksChecked}`);
      logger.info(`File records missing: ${stats.missingFileRecord}`);
      if (options.checkFs) {
        logger.info(`File paths missing: ${stats.missingFilePath}`);
      } else {
        logger.info("File paths missing: (filesystem check skipped)");
      }
      if (options.checkStatus) {
        logger.info(`File status issues: ${stats.statusIssues}`);
      } else {
        logger.info("File status issues: (status check skipped)");
      }
      logger.info(`Links OK: ${stats.ok}`);
    }
  }

  if (options.datasetPath) {
    logger.info("--- Starting Dataset ↔ DB Cross-Check ---");
    const datasetIndex = await loadDatasetIndex(
      options.datasetPath,
      nipFilter,
    );

    const statsDataset = {
      datasetRecords: 0,
      docsChecked: 0,
      missingJabatan: 0,
      missingFileLink: 0,
      missingFileRecord: 0,
      missingFilePath: 0,
      ok: 0,
    };

    for (const [key, records] of datasetIndex.entries()) {
      statsDataset.datasetRecords += records.length;
      const [nip, tmtString] = key.split("__");
      const jabatanCandidates =
        jabatanByKey.get(key) ||
        jabatanByKey.get(`${nip}__${tmtString}`) ||
        [];

      if (!jabatanCandidates || jabatanCandidates.length === 0) {
        statsDataset.missingJabatan += 1;
        logger.error(
          `[DATASET_MISSING] NIP ${nip} / TMT ${tmtString}: jabatan row not found.`,
        );
        continue;
      }

      for (const record of records) {
        if (!record.path || typeof record.path !== "object") continue;
        for (const [docId, fileInfo] of Object.entries(record.path)) {
          if (!fileInfo || !fileInfo.dok_uri) continue;
          statsDataset.docsChecked += 1;

          const columnInfo = FILE_COLUMNS.find((col) =>
            col.datasetDocs.includes(String(docId)),
          );
          if (!columnInfo) continue;

          const keepRow = chooseBestJabatanRow(
            jabatanCandidates,
            columnInfo.column,
          );
          if (!keepRow || !keepRow[columnInfo.column]) {
            statsDataset.missingFileLink += 1;
            logger.warn(
              `[DATASET_LINK] NIP ${nip} / TMT ${record.tmtJabatan} / ${columnInfo.label}: jabatan file column empty or jabatan missing.`,
            );
            continue;
          }

          const fileRecord = fileMap.get(keepRow[columnInfo.column]);
          if (!fileRecord) {
            statsDataset.missingFileRecord += 1;
            logger.error(
              `[DATASET_FILE] NIP ${nip} / TMT ${record.tmtJabatan} / ${columnInfo.label}: file_id ${keepRow[columnInfo.column]} not found.`,
            );
            continue;
          }

          if (
            options.checkFs &&
            fileRecord.file_path &&
            !fs.existsSync(fileRecord.file_path)
          ) {
            statsDataset.missingFilePath += 1;
            logger.warn(
              `[DATASET_PATH] NIP ${nip} / TMT ${record.tmtJabatan} / ${columnInfo.label}: path missing (${fileRecord.file_path}).`,
            );
            continue;
          }

          statsDataset.ok += 1;
          if (options.verbose) {
            logger.info(
              `[DATASET_OK] NIP ${nip} / TMT ${record.tmtJabatan} / ${columnInfo.label}: file_id ${keepRow[columnInfo.column]} -> ${fileRecord.file_path}`,
            );
          }
        }
      }
    }

    logger.info("--- Dataset Cross-Check Summary ---");
    logger.info(`Dataset records scanned: ${statsDataset.datasetRecords}`);
    logger.info(`Documents checked: ${statsDataset.docsChecked}`);
    logger.info(`Missing jabatan rows: ${statsDataset.missingJabatan}`);
    logger.info(`Missing jabatan file links: ${statsDataset.missingFileLink}`);
    logger.info(`Missing file records: ${statsDataset.missingFileRecord}`);
    if (options.checkFs) {
      logger.info(`Missing file paths: ${statsDataset.missingFilePath}`);
    } else {
      logger.info("Missing file paths: (filesystem check skipped)");
    }
    logger.info(`Dataset links OK: ${statsDataset.ok}`);
  }

  logger.info("--- Validation Finished ---");
}

if (require.main === module) {
  main()
    .catch((err) => {
      logger.error(`[FATAL] Validation failed: ${err.message}`);
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
