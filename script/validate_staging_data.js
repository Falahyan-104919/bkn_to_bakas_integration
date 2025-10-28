// 1. Imports
const fs = require("fs");
const fsp = fs.promises;
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

// 3. Helpers
function sanitizeString(str) {
  if (!str) return null;
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

// 4. Validation routines
async function validateJsonStructure(filePath) {
  const issues = [];
  let parsed;

  try {
    const raw = await fsp.readFile(filePath, "utf-8");
    parsed = JSON.parse(sanitizeString(raw));
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

async function validateRecordFiles(record, nip) {
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
    } catch (err) {
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

async function validateRecord(record, nip) {
  const issues = [];
  const warnings = [];

  if (!record.id || !record.tmtJabatan) {
    issues.push("Missing critical fields (id or tmtJabatan)");
  }

  if (!parseDate(record.tmtJabatan)) {
    issues.push(`Invalid tmtJabatan date format "${record.tmtJabatan}"`);
  }

  if (record.tanggalSk && !parseDate(record.tanggalSk)) {
    warnings.push(`Invalid tanggalSk "${record.tanggalSk}"`);
  }

  const pathCheck = await validateRecordFiles(record, nip);
  issues.push(...pathCheck.issues);
  warnings.push(...pathCheck.warnings);

  return { issues, warnings };
}

// 5. Main CLI
async function validateNipFile(nip) {
  const filePath = path.join(STAGING_DATA_DIR, `${nip}.json`);

  const { parsed, issues: jsonIssues } = await validateJsonStructure(filePath);
  const problems = [...jsonIssues];
  const warnings = [];

  if (!parsed) {
    return { nip, problems, warnings, recordCount: 0 };
  }

  let recordCount = 0;

  for (const record of parsed.data) {
    recordCount += 1;
    const { issues: recordIssues, warnings: recordWarnings } =
      await validateRecord(record, nip);

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

  return { nip, problems, warnings, recordCount };
}

async function main() {
  const [, , nipArg] = process.argv;

  const nips = [];
  if (nipArg) {
    nips.push(nipArg);
  } else {
    const allFiles = await fsp.readdir(STAGING_DATA_DIR);
    for (const file of allFiles) {
      if (file.endsWith(".json")) {
        nips.push(path.basename(file, ".json"));
      }
    }
  }

  if (nips.length === 0) {
    logger.info("No JSON inputs found; nothing to validate.");
    return;
  }

  let fatalCount = 0;
  let warningCount = 0;

  for (const nip of nips) {
    const result = await validateNipFile(nip);
    logger.info(
      `NIP ${nip}: ${result.recordCount} record(s) checked. ${result.problems.length} issue(s), ${result.warnings.length} warning(s).`,
    );

    for (const issue of result.problems) {
      fatalCount += 1;
      logger.error(`  [ERROR] ${issue}`);
    }

    for (const warning of result.warnings) {
      warningCount += 1;
      logger.warn(`  [WARN] ${warning}`);
    }
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
