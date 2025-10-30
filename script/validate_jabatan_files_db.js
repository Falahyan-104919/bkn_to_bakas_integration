#!/usr/bin/env node

require("dotenv").config();

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const { PrismaClient, Prisma } = require("@prisma/client");
const logger = require("./logger");

const prisma = new PrismaClient();

const FILE_COLUMNS = [
  { column: "trx_jabatan_file_id", label: "skJabatan" },
  { column: "trx_jabatan_file_spp", label: "spPelantikan" },
  { column: "trx_jabatan_file_ba", label: "baJabatan" },
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

  let nipFilter;
  try {
    nipFilter = await resolveNipFilter(options);
  } catch (err) {
    logger.error(`[ARGS] ${err.message}`);
    process.exitCode = 1;
    return;
  }

  logger.info(
    `--- Starting DB Jabatan/File Validation (filters=${
      nipFilter ? `${nipFilter.size} NIP(s)` : "none"
    }) ---`,
  );

  const jabatanRows = await fetchJabatanRows(nipFilter);
  if (jabatanRows.length === 0) {
    logger.warn("[CHECK] No jabatan rows matched the filters.");
    return;
  }

  const fileIds = new Set();
  for (const row of jabatanRows) {
    FILE_COLUMNS.forEach(({ column }) => {
      const value = row[column];
      if (value) fileIds.add(value);
    });
  }

  const fileMap = await fetchFileRecords(Array.from(fileIds));

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
      ? row.trx_jabatan_tmt.toISOString().slice(0, 10)
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

  logger.info("--- Validation Summary ---");
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
