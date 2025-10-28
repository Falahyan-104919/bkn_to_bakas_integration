// 1. IMPORTS
const fsp = require("fs").promises;
const fs = require("fs");
const path = require("path");
// --- TODO: 1. UPDATE YOUR PRISMA IMPORT ---
const { PrismaClient } = require("@prisma/client"); // Use this for default
const logger = require("./logger");

const prisma = new PrismaClient();

// --- CONFIGURATION ---
const STAGING_DATA_DIR = path.join(__dirname, "..", "staging_data");
const STAGING_FILES_DIR = path.join(__dirname, "..", "temp_downloads");
const SUPERADMIN_ID = 1;
const STATUS_SYNC_BKN = 3;

// --- TODO: 2. SET YOUR PRODUCTION FILE PATH ---
const FINAL_FILE_DESTINATION_BASE = "/home/aptika/sinetron-back/assets/upload";
const DEFAULT_DATASET_FILENAME = "1-final.json";

// --- TODO: 3. SET BKN 'dok_id' TO LOCAL 'fileKey' MAPPING ---
const BKN_DOC_ID_TO_FILE_KEY = {
  872: "skJabatan",
  873: "spPelantikan",
};

// --- This is your mapping from 'fileKey' to DB column info ---
const LOCAL_FILE_KEY_MAPPING = {
  skJabatan: { fileType: 11, field: "trx_jabatan_file_id" },
  spPelantikan: { fileType: 40, field: "trx_jabatan_file_spp" },
  baJabatan: { fileType: 41, field: "trx_jabatan_file_ba" },
};

// --- HELPER FUNCTIONS ---

/**
 * Removes non-UTF-8 characters from a string.
 * @param {string} str
 * @returns {string}
 */
function sanitizeString(str) {
  if (!str) return null;
  // This regex replaces invalid multi-byte UTF-8 sequences with an empty string.
  return str.replace(/[\uFFFD]/g, "");
}

/**
 * Parses "DD-MM-YYYY" string to a Date object.
 * @param {string} dateString
 * @returns {Date | null}
 */
function parseDate(dateString) {
  if (!dateString || typeof dateString !== "string") return null;
  const [dayString, monthString, yearString] = dateString.split("-");
  if (
    !dayString ||
    !monthString ||
    !yearString ||
    yearString.length !== 4 ||
    dayString.length !== 2 ||
    monthString.length !== 2
  ) {
    return null;
  }

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

/**
 * Finds the correct Jabatan Kode based on your logic.
 * @param {object} record - The BKN JSON record
 * @returns {string | null} The Jabatan Kode
 */
function findJabatanKode(record) {
  switch (record.jenisJabatan) {
    case "1":
      return record.unorId;
    case "2":
      return record.jabatanFungsionalId;
    case "4":
      return record.jabatanFungsionalUmumId;
    default:
      return null;
  }
}

function findJabatanJenjang(record) {
  switch (record.jenisJabatan) {
    case "1":
      return 3;
    case "2":
      return 2;
    case "4":
      return 1;
    default:
      return null;
  }
}

/**
 * Cleans a string to be used in a filename.
 * @param {string} name
 * @returns {string}
 */
function sanitizeFileName(name) {
  if (!name) return "UNKNOWN";
  return name
    .toUpperCase()
    .replace(/\s+/g, "_")
    .replace(/[^A-Z0-9_]/g, "");
}

/**
 * Returns true when the value is null/undefined or a string that trims to empty.
 * @param {*} value
 * @returns {boolean}
 */
function isBlank(value) {
  return (
    value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "")
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

/**
 * Processes an array of records for a given NIP.
 * @param {string} nip
 * @param {Array<object>} records
 * @returns {Promise<boolean>}
 */
async function processRecordsForNip(nip, records) {
  if (!records || !Array.isArray(records) || records.length === 0) {
    logger.warn(`No history records found or data is not an array for NIP: ${nip}`);
    return false;
  }

  logger.info(`Processing NIP: ${nip} (${records.length} records)`);

  for (const record of records) {
    const fileMoveOps = [];

    try {
      if (
        isBlank(record.unorId) &&
        isBlank(record.namaUnor) &&
        isBlank(record.unorIndukId) &&
        isBlank(record.unorIndukNama) &&
        isBlank(record.jabatanFungsionalId) &&
        isBlank(record.jabatanFungsionalNama) &&
        isBlank(record.jabatanFungsionalUmumId) &&
        isBlank(record.jabatanFungsionalUmumNama) &&
        isBlank(record.namaJabatan) &&
        isBlank(record.nomorSk) &&
        isBlank(record.tanggalSk) &&
        isBlank(record.namaUnor)
      ) {
        logger.warn(
          `[SKIP] Record ${record.id} (NIP ${nip}) missing jabatan/organization metadata. Skipping.`,
        );
        continue;
      }

      const employee = await prisma.ms_employee.findFirst({
        where: {
          employee_nip: record.nipBaru,
          employee_status: { notIn: [0] },
        },
      });
      if (!employee) {
        throw new Error(`NIP ${record.nipBaru} not found in local ms_employee`);
      }

      const jabatanKode = findJabatanKode(record);
      const jabatanJenjang = findJabatanJenjang(record);
      const jabatan = jabatanKode
        ? await prisma.ms_jabatan.findFirst({
            where: {
              jabatan_kode: jabatanKode,
              jabatan_status: { notIn: [0] },
            },
          })
        : null;

      const organization = await prisma.ms_organization.findFirst({
        where: {
          organization_bkn_id: record.unorId,
          organization_status: { notIn: [0] },
        },
      });

      const parsedTmtJabatan = parseDate(record.tmtJabatan);
      if (!parsedTmtJabatan) {
        logger.warn(
          `[SKIP] Invalid or missing TMT for record ${record.id} (NIP ${nip}).`,
        );
        continue;
      }
      const dataPayload = {
        trx_jabatan_jabatan_id: jabatan ? jabatan.jabatan_id : null,
        trx_jabatan_organization_id: organization
          ? organization.organization_id
          : null,
        trx_jabatan_nomor_sk: sanitizeString(record.nomorSk),
        trx_jabatan_tgl_sk: parseDate(record.tanggalSk),
        trx_jabatan_status: STATUS_SYNC_BKN,
        trx_jabatan_jenis_sk: 3,
        trx_jabatan_pejabat_sk: null,
        trx_jabatan_status_jabatan: 1,
        trx_jabatan_jabatan_nama: !jabatan ? record.namaJabatan : null,
        trx_jabatan_jabatan_eselon: null,
        trx_jabatan_jabatan_organization: !organization
          ? record.namaUnor
          : null,
        trx_jabatan_file_ba: null,
        trx_jabatan_type: jabatanJenjang,

        ...(record.satuanKerjaId === "A5EB03E241B3F6A0E040640A040252AD" && {
          trx_jabatan_instansi_type: 3,
          trx_jabatan_instansi: 129,
        }),
      };

      const fileCreateDataMap = new Map();

      if (record.path && typeof record.path === "object") {
        for (const [docKey, fileInfo] of Object.entries(record.path)) {
          const fileKeyName = BKN_DOC_ID_TO_FILE_KEY[docKey];
          if (!fileKeyName) continue;

          if (
            !fileInfo ||
            !fileInfo.dok_uri ||
            typeof fileInfo.dok_uri !== "string"
          ) {
            logger.warn(
              `[FILE] Missing or invalid dok_uri for doc ${docKey} on record ${record.id}.`,
            );
            continue;
          }

          const basename = path.basename(fileInfo.dok_uri);
          if (!basename) {
            logger.warn(
              `[FILE] Unable to resolve filename from dok_uri for doc ${docKey} on record ${record.id}.`,
            );
            continue;
          }

          const safeDownloadedFilename = `${record.id}_${docKey}_${basename}`;
          const sourcePath = path.join(
            STAGING_FILES_DIR,
            safeDownloadedFilename,
          );

          if (!fs.existsSync(sourcePath)) {
            logger.warn(
              `[FILE] File not found in temp_downloads: ${safeDownloadedFilename}`,
            );
            continue;
          }

          const jabatanNamaPart = sanitizeFileName(record.namaJabatan);
          const dateString = parseDate(record.tanggalSk) || new Date();
          const datePart = `${String(dateString.getDate()).padStart(2, "0")}${String(dateString.getMonth() + 1).padStart(2, "0")}${String(dateString.getFullYear()).slice(2)}`;
          const fileKeyPart = fileKeyName;

          const newFilename = `${nip}_${jabatanNamaPart}_${datePart}_${fileKeyPart}.pdf`;
          const finalDirPath = path.join(FINAL_FILE_DESTINATION_BASE, nip);
          const finalFilePath = path.join(finalDirPath, newFilename);

          const fileMapping = LOCAL_FILE_KEY_MAPPING[fileKeyName];
          if (!fileMapping) {
            logger.warn(
              `[FILE] No LOCAL_FILE_KEY_MAPPING entry for key ${fileKeyName} (doc ${docKey}).`,
            );
            continue;
          }
          const stats = await fsp.stat(sourcePath);

          fileCreateDataMap.set(fileKeyName, {
            fileMapping: fileMapping,
            createData: {
              file_employee_id: employee.employee_id,
              file_name: newFilename,
              file_type: fileMapping.fileType,
              file_path: finalFilePath,
              file_status: 1,
              file_create_by: SUPERADMIN_ID,
              file_create_date: new Date(),
              file_size: stats.size,
              file_ext: "pdf",
            },
          });

          fileMoveOps.push({ sourcePath, finalFilePath, finalDirPath });
        }
      }

      await prisma.$transaction(async (tx) => {
        const uniqueWhere = {
          trx_jabatan_employee_id_trx_jabatan_tmt: {
            trx_jabatan_employee_id: employee.employee_id,
            trx_jabatan_tmt: parsedTmtJabatan,
          },
        };

        const jabatanRecord = await tx.trx_jabatan.upsert({
          where: uniqueWhere,
          update: {
            ...dataPayload,
          },
          create: {
            ...dataPayload,
            trx_jabatan_employee_id: employee.employee_id,
            trx_jabatan_tmt: parsedTmtJabatan,
            trx_jabatan_create_date: new Date(),
            trx_jabatan_create_by: 0,
          },
        });

        if (fileCreateDataMap.size === 0) {
          return;
        }

        const fileIdsToLink = {};

        for (const [, fileData] of fileCreateDataMap.entries()) {
          const newFileRecord = await tx.trx_employee_file.create({
            data: fileData.createData,
          });

          fileIdsToLink[fileData.fileMapping.field] = newFileRecord.file_id;
        }

        if (Object.keys(fileIdsToLink).length > 0) {
          await tx.trx_jabatan.update({
            where: {
              trx_jabatan_id: jabatanRecord.trx_jabatan_id,
            },
            data: fileIdsToLink,
          });
        }

        logger.info(
          `[UPSERT] Upserted record for NIP ${nip} / TMT ${record.tmtJabatan}.`,
        );
      });

      for (const op of fileMoveOps) {
        await fsp.mkdir(op.finalDirPath, { recursive: true });
        if (fs.existsSync(op.finalFilePath)) {
          await fsp.unlink(op.finalFilePath);
          logger.warn(
            `[FILE_MOVE] Existing file replaced at: ${op.finalFilePath}`,
          );
        }
        await fsp.rename(op.sourcePath, op.finalFilePath);
        logger.info(`[FILE_MOVE] Moved file to: ${op.finalFilePath}`);
      }

      logger.info(`[SUCCESS] Processed record ${record.id} for NIP ${nip}`);
    } catch (e) {
      logger.error(
        `[FAIL] Failed record ${record.id} for NIP ${nip}: ${e.message}`,
      );
      logger.error(e.stack);
    }
  }

  return true;
}

function parseCliArgs(argv) {
  const options = {
    datasetPath: null,
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
    options.datasetPath = null;
    if (options.nips.length === 0 && fs.existsSync(defaultCandidate)) {
      logger.info(
        `[CONFIG] Detected ${DEFAULT_DATASET_FILENAME}. Run with '--dataset ${path.relative(process.cwd(), defaultCandidate)}' to use the merged dataset.`,
      );
    }
  }

  return options;
}

function printHelp() {
  const lines = [
    "Usage: node script/importer.js [options] [NIP ...]",
    "",
    "Options:",
    "  --dataset <path>   Import from a merged JSON array (e.g. staging_data/1-final.json).",
    "  --help             Show this message.",
    "",
    "Without --dataset the importer reads per-NIP JSON files under staging_data.",
    "Provide one or more NIP arguments to limit processing to specific employees.",
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

/**
 * Processes a single staging JSON file identified by NIP.
 * @param {string} nip
 * @returns {Promise<boolean>} true when the file existed and was processed
 */
async function processNip(nip) {
  const filePath = path.join(STAGING_DATA_DIR, `${nip}.json`);

  let fileContent;
  try {
    fileContent = await fsp.readFile(filePath, "utf-8");
  } catch (err) {
    logger.error(
      `[FAIL] Unable to read staging file for NIP ${nip}: ${err.message}`,
    );
    return false;
  }

  let parsed;
  try {
    parsed = JSON.parse(fileContent);
  } catch (err) {
    logger.error(
      `[FAIL] Invalid JSON structure for NIP ${nip}: ${err.message}`,
    );
    return false;
  }

  const records = Array.isArray(parsed?.data)
    ? parsed.data
    : Array.isArray(parsed)
      ? parsed
      : null;

  return processRecordsForNip(nip, records);
}

/**
 * Main Importer Function
 */
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

  logger.info("--- Starting Standalone Importer (Jabatan Upsert) Script ---");

  if (options.datasetPath) {
    const datasetAbsolute = path.resolve(process.cwd(), options.datasetPath);
    let datasetRecords;
    try {
      datasetRecords = await loadDatasetRecords(datasetAbsolute);
    } catch (err) {
      logger.error(`[FAIL] Unable to read dataset ${datasetAbsolute}: ${err.message}`);
      throw err;
    }

    const nipFilter =
      options.nips.length > 0
        ? new Set(options.nips.map((nip) => nip.trim()).filter(Boolean))
        : null;

    const grouped = groupRecordsByNip(datasetRecords, nipFilter);

    if (grouped.size === 0) {
      logger.warn("[DATASET] No records matched the provided filters.");
    }

    for (const [nip, records] of grouped.entries()) {
      await processRecordsForNip(nip, records);
    }
  } else {
    const files = await fsp.readdir(STAGING_DATA_DIR);
    const nipFilter =
      options.nips.length > 0
        ? new Set(options.nips.map((nip) => nip.trim()).filter(Boolean))
        : null;

    for (const file of files) {
      if (!file.endsWith(".json")) continue;

      const nip = path.basename(file, ".json");
      if (nipFilter && !nipFilter.has(nip)) continue;

      await processNip(nip);
    }
  }

  logger.info("--- Importer Script Finished ---");
}

// --- RUN THE SCRIPT ---
if (require.main === module) {
  main()
    .catch((e) => {
      logger.error(
        `[FATAL] The script encountered a fatal error: ${e.message}`,
      );
      process.exit(1);
    })
    .finally(async () => {
      await prisma.$disconnect();
      logger.info("--- Database disconnected ---");
    });
}

module.exports = {
  main,
  processNip,
  processRecordsForNip,
  parseDate,
  findJabatanKode,
  prisma,
};
