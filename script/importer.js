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

  let bknHistoryRecords;
  try {
    bknHistoryRecords = JSON.parse(fileContent).data;
  } catch (err) {
    logger.error(
      `[FAIL] Invalid JSON structure for NIP ${nip}: ${err.message}`,
    );
    return false;
  }

  if (
    !bknHistoryRecords ||
    !Array.isArray(bknHistoryRecords) ||
    bknHistoryRecords.length === 0
  ) {
    logger.warn(
      `No history records found or data is not an array for NIP: ${nip}`,
    );
    return false;
  }

  logger.info(`Processing NIP: ${nip} (${bknHistoryRecords.length} records)`);

  for (const record of bknHistoryRecords) {
    // This will hold { sourcePath, finalFilePath } for moving files *after* the TX
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

      // --- 1. GATHER DATA (Lookups) ---
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

      // --- 2. PREPARE JABATAN DATA (The 'payload' for create/update) ---
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
        trx_jabatan_nomor_sk: record.nomorSk,
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

      // --- 3. PREPARE FILE DATA (Pre-Transaction) ---
      // This holds the data for creating new 'trx_employee_file' records
      const fileCreateDataMap = new Map();

      if (record.path) {
        for (const [docKey, fileInfo] of Object.entries(record.path)) {
          const fileKeyName = BKN_DOC_ID_TO_FILE_KEY[docKey];
          if (!fileKeyName) continue; // Skip unmapped files

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

          // --- TODO: 4. MODIFY YOUR FILE NAMING LOGIC HERE ---
          const jabatanNamaPart = sanitizeFileName(record.namaJabatan);
          const dateString = parseDate(record.tanggalSk) || new Date();
          const datePart = `${String(dateString.getDate()).padStart(2, "0")}${String(dateString.getMonth() + 1).padStart(2, "0")}${String(dateString.getFullYear()).slice(2)}`;
          const fileKeyPart = fileKeyName;

          const newFilename = `${nip}_${jabatanNamaPart}_${datePart}_${fileKeyPart}.pdf`;
          const finalDirPath = path.join(FINAL_FILE_DESTINATION_BASE, nip);
          const finalFilePath = path.join(finalDirPath, newFilename);
          // --- End of TODO section ---

          const fileMapping = LOCAL_FILE_KEY_MAPPING[fileKeyName];
          if (!fileMapping) {
            logger.warn(
              `[FILE] No LOCAL_FILE_KEY_MAPPING entry for key ${fileKeyName} (doc ${docKey}).`,
            );
            continue;
          }
          const stats = await fsp.stat(sourcePath);

          // Store the data needed to create this file record
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

          // Store the operation to move the file after the transaction
          fileMoveOps.push({ sourcePath, finalFilePath, finalDirPath });
        }
      }

      // --- 4. START TRANSACTION ---
      await prisma.$transaction(async (tx) => {
        const fileIdsToLink = {}; // { trx_jabatan_file_id: 123, ... }

        // --- FIX: PROCESS FILES *FIRST* (FOR BOTH CREATE AND UPDATE) ---
        for (const [fileKeyName, fileData] of fileCreateDataMap.entries()) {
          // We UPSERT the file record. This creates it if it's new, or
          // updates it with the new filename/path if it already exists.
          // --- CRITICAL ASSUMPTION: Requires a unique key on [file_employee_id, file_type] ---
          // In your schema.prisma: @@unique([file_employee_id, file_type])
          const newFileRecord = await tx.trx_employee_file.upsert({
            where: {
              file_employee_id_file_type: {
                // <-- Assumed unique index name
                file_employee_id: employee.employee_id,
                file_type: fileData.fileMapping.fileType,
              },
            },
            update: fileData.createData, // Update with new name, path, size
            create: fileData.createData, // Create if it doesn't exist
          });

          // Save the new/updated file ID to link to the jabatan
          fileIdsToLink[fileData.fileMapping.field] = newFileRecord.file_id;
        }

        // --- Now, find the existing jabatan ---
        const uniqueWhere = {
          trx_jabatan_employee_id_trx_jabatan_tmt: {
            trx_jabatan_employee_id: employee.employee_id,
            trx_jabatan_tmt: parsedTmtJabatan,
          },
        };

        await tx.trx_jabatan.upsert({
          where: uniqueWhere,
          update: {
            ...dataPayload,
            ...fileIdsToLink,
          },
          create: {
            ...dataPayload,
            trx_jabatan_employee_id: employee.employee_id,
            trx_jabatan_tmt: parsedTmtJabatan,
            trx_jabatan_create_date: new Date(),
            trx_jabatan_create_by: 0,
            ...fileIdsToLink,
          },
        });
        logger.info(
          `[UPSERT] Upserted record for NIP ${nip} / TMT ${record.tmtJabatan}.`,
        );
      }); // --- END TRANSACTION ---

      // --- 5. MOVE FILES (Post-Transaction) ---
      for (const op of fileMoveOps) {
        if (fs.existsSync(op.finalFilePath)) {
          logger.warn(
            `[FILE_MOVE] File already exists, skipping move: ${op.finalFilePath}`,
          );
        } else {
          await fsp.mkdir(op.finalDirPath, { recursive: true });
          await fsp.rename(op.sourcePath, op.finalFilePath); // Use rename (move)
          logger.info(`[FILE_MOVE] Moved file to: ${op.finalFilePath}`);
        }
      }

      logger.info(`[SUCCESS] Processed record ${record.id} for NIP ${nip}`);
    } catch (e) {
      logger.error(
        `[FAIL] Failed record ${record.id} for NIP ${nip}: ${e.message}`,
      );
      logger.error(e.stack); // Log the full stack trace for debugging
    }
  }

  return true;
}

/**
 * Main Importer Function
 */
async function main() {
  logger.info("--- Starting Standalone Importer (Jabatan Upsert) Script ---");

  const files = await fsp.readdir(STAGING_DATA_DIR);

  for (const file of files) {
    if (!file.endsWith(".json")) continue;

    const nip = path.basename(file, ".json");
    await processNip(nip);
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
  parseDate,
  findJabatanKode,
  prisma,
};
