#!/usr/bin/env node

// 1. Imports
const fsp = require("fs").promises;
const path = require("path");
const logger = require("./logger");

// 2. Configuration (keep in sync with importer.js)
const STAGING_DATA_DIR = path.join(__dirname, "..", "staging_data");
const STAGING_FILES_DIR = path.join(__dirname, "..", "temp_downloads");

const BKN_DOC_ID_TO_FILE_KEY = {
  872: "skJabatan",
  873: "spPelantikan",
};

const LOCAL_FILE_KEY_MAPPING = {
  skJabatan: { fileType: 11, field: "trx_jabatan_file_id" },
  spPelantikan: { fileType: 40, field: "trx_jabatan_file_spp" },
  baJabatan: { fileType: 41, field: "trx_jabatan_file_ba" },
};

// Populate this array when you want --errors-only to work without --ids.
const DEFAULT_PROBLEM_RECORD_IDS = [];

// 3. Helpers
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

async function checkPdfSignature(filePath) {
  const fh = await fsp.open(filePath, "r");
  try {
    const buffer = Buffer.alloc(5);
    const { bytesRead } = await fh.read(buffer, 0, 5, 0);
    if (bytesRead < 4) return false;
    return buffer.toString("utf-8", 0, 4) === "%PDF";
  } finally {
    await fh.close();
  }
}

function buildTempFilename(recordId, docKey, dokUri) {
  const basename = path.basename(dokUri || "");
  if (!basename) return null;
  return `${recordId}_${docKey}_${basename}`;
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

function parseCliArgs(argv) {
  const options = {
    datasetPath: null,
    errorsOnly: false,
    problemIdsPath: null,
    nips: [],
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
      case "--ids":
        if (i + 1 >= argv.length) {
          throw new Error("--ids requires a path argument.");
        }
        options.problemIdsPath = argv[++i];
        break;
      case "--errors-only":
        options.errorsOnly = true;
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

  return options;
}

function printHelp() {
  const lines = [
    "Usage: node script/validate_staging_data.js [options] [NIP ...]",
    "",
    "Options:",
    "  --dataset <path>   Validate records from a merged JSON file.",
    "  --errors-only      Restrict validation to problematic record IDs.",
    "  --ids <path>       Load problematic record IDs from a JSON array file.",
    "  --help             Show this message.",
    "",
    "Without --dataset the script validates staging_data/<NIP>.json files.",
    "When --errors-only is supplied, records outside the ID list are skipped.",
  ];
  lines.forEach((line) => logger.info(line));
}

async function getProblemRecordIdSet(idsPath) {
  if (idsPath) {
    const absolute = path.resolve(process.cwd(), idsPath);
    const raw = await fsp.readFile(absolute, "utf-8");
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      throw new Error("--ids file must contain a JSON array of record IDs.");
    }
    return new Set(parsed.map((value) => String(value).trim()).filter(Boolean));
  }

  if (DEFAULT_PROBLEM_RECORD_IDS.length > 0) {
    return new Set(DEFAULT_PROBLEM_RECORD_IDS);
  }

  return null;
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

function groupRecordsByNip(records, { nipFilterSet, problemIdSet } = {}) {
  const groups = new Map();

  for (const record of records) {
    if (!record || typeof record !== "object") continue;

    if (problemIdSet && problemIdSet.size > 0) {
      if (!record.id || !problemIdSet.has(record.id)) continue;
    }

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

async function discoverNipFiles() {
  const files = await fsp.readdir(STAGING_DATA_DIR);
  return files
    .filter((file) => file.endsWith(".json"))
    .map((file) => path.basename(file, ".json"))
    .sort();
}

async function validateJsonStructure(filePath) {
  const issues = [];
  let parsed;

  try {
    const raw = await fsp.readFile(filePath, "utf-8");
    const sanitized = sanitizeString(raw) ?? raw;
    parsed = JSON.parse(sanitized);
  } catch (err) {
    issues.push(`JSON parse failed: ${err.message}`);
    return { parsed: null, issues };
  }

  if (!parsed || !Array.isArray(parsed.data)) {
    issues.push("Missing or invalid 'data' array");
    return { parsed: null, issues };
  }

  return { parsed, issues };
}

async function validateRecordFiles(record) {
  const issues = [];
  const warnings = [];

  if (!record.path || Object.keys(record.path).length === 0) {
    warnings.push("No 'path' entries present");
    return { issues, warnings };
  }

  for (const [docKey, fileInfo] of Object.entries(record.path)) {
    const fileKeyName = BKN_DOC_ID_TO_FILE_KEY[docKey];
    if (!fileKeyName) {
      warnings.push(`Doc ${docKey} has no mapping; skipping`);
      continue;
    }

    if (!LOCAL_FILE_KEY_MAPPING[fileKeyName]) {
      issues.push(`Doc ${docKey} maps to unknown local key '${fileKeyName}'`);
      continue;
    }

    if (!fileInfo || typeof fileInfo.dok_uri !== "string") {
      issues.push(`Doc ${docKey} missing valid 'dok_uri'`);
      continue;
    }

    const tempFilename = buildTempFilename(record.id, docKey, fileInfo.dok_uri);
    if (!tempFilename) {
      issues.push(`Doc ${docKey} could not derive staging filename`);
      continue;
    }

    const sourcePath = path.join(STAGING_FILES_DIR, tempFilename);

    try {
      const stats = await fsp.stat(sourcePath);
      if (!stats.isFile()) {
        issues.push(`Doc ${docKey} staging entry is not a regular file`);
        continue;
      }
      if (stats.size === 0) {
        issues.push(`Doc ${docKey} file size is zero bytes`);
        continue;
      }
    } catch {
      issues.push(`Doc ${docKey} missing staging file (${tempFilename})`);
      continue;
    }

    try {
      const isPdf = await checkPdfSignature(sourcePath);
      if (!isPdf) {
        warnings.push(
          `Doc ${docKey} (${tempFilename}) does not start with %PDF-; check manually`,
        );
      }
    } catch (err) {
      warnings.push(
        `Doc ${docKey} (${tempFilename}) could not read header: ${err.message}`,
      );
    }
  }

  return { issues, warnings };
}

async function validateRecord(record) {
  const issues = [];
  const warnings = [];

  if (!record || typeof record !== "object") {
    issues.push("Record is missing or not an object");
    return { issues, warnings };
  }

  if (!record.id || !record.tmtJabatan) {
    issues.push("Missing critical fields (id or tmtJabatan)");
  }

  if (!parseDate(record.tmtJabatan)) {
    issues.push(`Invalid tmtJabatan date format "${record.tmtJabatan}"`);
  }

  if (record.tanggalSk && !parseDate(record.tanggalSk)) {
    warnings.push(`Invalid tanggalSk "${record.tanggalSk}"`);
  }

  const pathCheck = await validateRecordFiles(record);
  issues.push(...pathCheck.issues);
  warnings.push(...pathCheck.warnings);

  return { issues, warnings };
}

async function validateRecordsForNip(nip, records, initialProblems = []) {
  const problems = [...initialProblems];
  const warnings = [];
  const processedRecordIds = [];
  let recordCount = 0;

  for (const record of records) {
    recordCount += 1;
    if (record && record.id) processedRecordIds.push(record.id);

    const { issues: recordIssues, warnings: recordWarnings } =
      await validateRecord(record);

    if (recordIssues.length > 0) {
      problems.push(
        `Record ${record.id || recordCount}: ${recordIssues.join("; ")}`,
      );
    }
    if (recordWarnings.length > 0) {
      warnings.push(
        `Record ${record.id || recordCount}: ${recordWarnings.join("; ")}`,
      );
    }
  }

  return { nip, problems, warnings, recordCount, processedRecordIds };
}

async function validateNipFile(nip, filterSet) {
  const filePath = path.join(STAGING_DATA_DIR, `${nip}.json`);

  const { parsed, issues: jsonIssues } = await validateJsonStructure(filePath);
  if (!parsed) {
    return {
      nip,
      problems: jsonIssues,
      warnings: [],
      recordCount: 0,
      processedRecordIds: [],
    };
  }

  const dataArray = parsed.data || [];
  const filteredRecords =
    filterSet && filterSet.size > 0
      ? dataArray.filter((record) => record && filterSet.has(record.id))
      : dataArray;

  return validateRecordsForNip(nip, filteredRecords, jsonIssues);
}

// 4. Main CLI
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

  let problemIdSet = null;
  if (options.errorsOnly) {
    problemIdSet = await getProblemRecordIdSet(options.problemIdsPath);
    if (!problemIdSet || problemIdSet.size === 0) {
      logger.warn(
        "[CONFIG] --errors-only requested but no problematic record IDs were supplied.",
      );
      return;
    }
    logger.info(
      `[CONFIG] Restricting validation to ${problemIdSet.size} problematic record ID(s).`,
    );
  }

  const unresolvedProblemIds = problemIdSet ? new Set(problemIdSet) : null;
  const targets = [];

  if (options.datasetPath) {
    const datasetAbsolute = path.resolve(process.cwd(), options.datasetPath);
    const datasetRecords = await loadDatasetRecords(datasetAbsolute);
    logger.info(
      `[DATASET] Loaded ${datasetRecords.length} record(s) from ${datasetAbsolute}`,
    );

    const nipFilterSet =
      options.nips.length > 0
        ? new Set(options.nips.map((nip) => nip.trim()).filter(Boolean))
        : null;

    const grouped = groupRecordsByNip(datasetRecords, {
      nipFilterSet,
      problemIdSet,
    });

    if (grouped.size === 0) {
      logger.warn(
        "[DATASET] No dataset records matched the provided filters; nothing to validate.",
      );
    }

    for (const [nip, records] of grouped.entries()) {
      targets.push({ nip, records });
    }
  } else {
    const nipList =
      options.nips.length > 0 ? options.nips : await discoverNipFiles();

    if (nipList.length === 0) {
      logger.info("[SCAN] No staging JSON files found; nothing to validate.");
      return;
    }

    if (options.errorsOnly && problemIdSet) {
      logger.info(
        "[CONFIG] Applying problematic record filter to per-NIP JSON files.",
      );
    }

    for (const nip of nipList) {
      targets.push({ nip, fromFile: true });
    }
  }

  let fatalCount = 0;
  let warningCount = 0;

  for (const target of targets) {
    let result;
    if (target.fromFile) {
      result = await validateNipFile(target.nip, problemIdSet);
    } else {
      result = await validateRecordsForNip(target.nip, target.records);
    }

    const didWork =
      result.recordCount > 0 ||
      result.problems.length > 0 ||
      result.warnings.length > 0;

    if (!didWork) {
      logger.info(
        `NIP ${target.nip}: 0 matching record(s) for current filters.`,
      );
      continue;
    }

    logger.info(
      `NIP ${result.nip}: ${result.recordCount} record(s) checked. ${result.problems.length} issue(s), ${result.warnings.length} warning(s).`,
    );

    for (const issue of result.problems) {
      fatalCount += 1;
      logger.error(`  [ERROR] ${issue}`);
    }

    for (const warning of result.warnings) {
      warningCount += 1;
      logger.warn(`  [WARN] ${warning}`);
    }

    if (unresolvedProblemIds && result.processedRecordIds) {
      for (const processedId of result.processedRecordIds) {
        if (processedId && unresolvedProblemIds.has(processedId)) {
          unresolvedProblemIds.delete(processedId);
        }
      }
    }
  }

  if (unresolvedProblemIds && unresolvedProblemIds.size > 0) {
    logger.warn(
      `[MISSING] ${unresolvedProblemIds.size} record ID(s) were not encountered: ${Array.from(unresolvedProblemIds).join(", ")}`,
    );
  }

  logger.info(
    `Validation finished. ${fatalCount} error(s), ${warningCount} warning(s).`,
  );

  if (fatalCount > 0) {
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main().catch((err) => {
    logger.error(`[FATAL] Validator crashed: ${err.message}`);
    logger.error(err.stack);
    process.exit(1);
  });
}
