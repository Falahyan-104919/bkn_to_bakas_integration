// 1. IMPORTS
const fsp = require("fs").promises; // For async file operations
const fs = require("fs"); // For checking file existence
const path = require("path");
const { PrismaClient } = require("@prisma/client");
const logger = require("./logger"); // Use our existing logger

const prisma = new PrismaClient();

// --- CONFIGURATION ---
const STAGING_DATA_DIR = path.join(__dirname, "staging_data");
const STAGING_FILES_DIR = path.join(__dirname, "temp_downloads");
const SUPERADMIN_ID = 1;
const STATUS_SYNC_BKN = 3;

// --- TODO: 1. SET YOUR PRODUCTION FILE PATH ---
// This is the *base* directory where files will be moved.
// Example: '/home/aptika/assets/upload'
const FINAL_FILE_DESTINATION_BASE = "/home/aptika/assets/upload";

// --- TODO: 2. SET BKN 'dok_id' TO LOCAL 'fileKey' MAPPING ---
// This maps BKN's file ID to your internal 'fileKey' (like 'skJabatan').
// Please add the dok_id for 'baJabatan' if you find it.
const BKN_DOC_ID_TO_FILE_KEY = {
  872: "skJabatan",
  873: "spPelantikan",
  // '874': 'baJabatan', // Example
};

// --- This is your mapping from 'fileKey' to DB column info ---
// We use this to know which file_type to set and which column to update.
const LOCAL_FILE_KEY_MAPPING = {
  skJabatan: { fileType: 11, field: "trx_jabatan_file_id" },
  spPelantikan: { fileType: 40, field: "trx_jabatan_file_spp" },
  baJabatan: { fileType: 41, field: "trx_jabatan_file_ba" },
};

// --- HELPER FUNCTIONS ---

/**
 * Parses "DD-MM-YYYY" string to "YYYY-MM-DD" string for Prisma.
 * @param {string} dateString
 * @returns {string | null}
 */
function parseDateForDB(dateString) {
  if (!dateString || dateString.length < 10) return null;
  const [day, month, year] = dateString.split("-");
  if (!day || !month || !year || year.length !== 4) return null;
  return `${year}-${month}-${day}`;
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

/**
 * Cleans a string to be used in a filename.
 * @param {string} name
 * @returns {string}
 */
function sanitizeFileName(name) {
  if (!name) return "UNKNOWN";
  return name
    .toUpperCase()
    .replace(/\s+/g, "_") // Replace spaces with underscores
    .replace(/[^A-Z0-9_]/g, ""); // Remove non-alphanumeric characters
}

/**
 * Main Importer Function
 */
async function main() {
  logger.info("--- Starting Standalone Importer Script ---");

  const files = await fsp.readdir(STAGING_DATA_DIR);

  for (const file of files) {
    if (!file.endsWith(".json")) continue;

    const nip = path.basename(file, ".json");
    const filePath = path.join(STAGING_DATA_DIR, file);

    const fileContent = await fsp.readFile(filePath, "utf-8");
    // We assume .data contains the array based on our previous fix
    const bknHistoryRecords = JSON.parse(fileContent).data;

    if (
      !bknHistoryRecords ||
      !Array.isArray(bknHistoryRecords) ||
      bknHistoryRecords.length === 0
    ) {
      logger.warn(
        `No history records found or data is not an array for NIP: ${nip}`,
      );
      continue;
    }

    logger.info(`Processing NIP: ${nip} (${bknHistoryRecords.length} records)`);

    for (const record of bknHistoryRecords) {
      // We will create a map of { trx_jabatan_file_id: 123, trx_jabatan_file_spp: 124 }
      const fileIdMap = {};

      try {
        // --- 1. GATHER DATA (Lookups) ---
        // These are done *outside* the transaction.
        const employee = await prisma.ms_employee.findUnique({
          where: { employee_nip: record.nipBaru },
        });
        if (!employee) {
          throw new Error(
            `NIP ${record.nipBaru} not found in local ms_employee`,
          );
        }

        const jabatanKode = findJabatanKode(record);
        const jabatan = jabatanKode
          ? await prisma.ms_jabatan.findUnique({
              where: { jabatan_kode: jabatanKode },
            })
          : null;

        const organization = await prisma.ms_organization.findUnique({
          where: { organization_bkn_id: record.unorId },
        });

        // --- 2. START TRANSACTION ---
        // This ensures that we either create the jabatan AND its files, or nothing.
        await prisma.$transaction(async (tx) => {
          // --- 3. PROCESS FILES (Move & Create 'trx_employee_file' records) ---
          if (record.path) {
            for (const [docKey, fileInfo] of Object.entries(record.path)) {
              // Find the local 'fileKey' (e.g., 'skJabatan')
              const fileKeyName = BKN_DOC_ID_TO_FILE_KEY[docKey];
              if (!fileKeyName) {
                logger.warn(
                  `[FILE] Skipping unmapped docKey: ${docKey} for NIP ${nip}`,
                );
                continue;
              }

              // Find the source file from our download folder
              const safeDownloadedFilename = `${record.id}_${docKey}_${path.basename(fileInfo.dok_uri)}`;
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

              // --- TODO: 3. MODIFY YOUR FILE NAMING LOGIC HERE ---
              // This logic is based on your example: [NIP]_[JABATAN_NAMA]_[DATE]_[FILE_KEY_NAME].pdf

              // 1. Jabatan Nama Part
              const jabatanNamaPart = sanitizeFileName(record.namaJabatan);

              // 2. Date Part (DDMMYY)
              // FIXME: This is a GUESS. You must confirm which date to use (e.g., tanggalSk)
              const dateString =
                parseDateForDB(record.tanggalSk) ||
                new Date().toISOString().split("T")[0];
              const [y, m, d] = dateString.split("-");
              const datePart = `${d}${m}${y.slice(2)}`; // e.g., "280314"

              // 3. File Key Part
              const fileKeyPart = fileKeyName; // e.g., 'skJabatan'

              const newFilename = `${nip}_${jabatanNamaPart}_${datePart}_${fileKeyPart}.pdf`;
              const finalDirPath = path.join(FINAL_FILE_DESTINATION_BASE, nip);
              const finalFilePath = path.join(finalDirPath, newFilename);

              // --- End of TODO section ---

              // Move the file
              await fsp.mkdir(finalDirPath, { recursive: true });
              await fsp.rename(sourcePath, finalFilePath); // Use rename for an atomic move

              // Create the trx_employee_file record
              const stats = await fsp.stat(finalFilePath);
              const fileMapping = LOCAL_FILE_KEY_MAPPING[fileKeyName];

              const newFileRecord = await tx.trx_employee_file.create({
                data: {
                  file_employee_id: employee.employee_id,
                  file_name: newFilename,
                  file_type: fileMapping.fileType,
                  file_path: finalFilePath, // The full production path
                  file_status: 1, // Assuming 1 = active
                  file_create_by: SUPERADMIN_ID,
                  file_create_date: new Date(),
                  file_size: stats.size,
                  file_ext: "pdf",
                },
              });

              // Save the new file_id to be added to the trx_jabatan record
              fileIdMap[fileMapping.field] = newFileRecord.file_id;
            }
          }

          // --- 4. BUILD FINAL 'trx_jabatan' DATA ---
          const dataJabatan = {
            trx_jabatan_employee_id: employee.employee_id,
            trx_jabatan_jabatan_id: jabatan ? jabatan.jabatan_id : null,
            trx_jabatan_organization_id: organization
              ? organization.organization_id
              : null,
            trx_jabatan_tmt: parseDateForDB(record.tmtJabatan),
            trx_jabatan_nomor_sk: record.nomorSk,
            trx_jabatan_tgl_sk: parseDateForDB(record.tanggalSk),
            trx_jabatan_create_date: new Date(),
            trx_jabatan_create_by: SUPERADMIN_ID,
            trx_jabatan_status: STATUS_SYNC_BKN,
            trx_jabatan_jenis_sk: 1,
            trx_jabatan_pejabat_sk: null,
            trx_jabatan_status_jabatan: 1,
            trx_jabatan_jabatan_nama: !jabatan ? record.namaJabatan : null,
            trx_jabatan_jabatan_eselon: null,
            trx_jabatan_jabatan_organization: !organization
              ? record.namaUnor
              : null,
            trx_jabatan_file_ba: null,
            trx_jabatan_type: 1,

            // Add conditional Instansi logic
            ...(record.satuanKerjaId === "A5EB03E241B3F6A0E040640A040252AD" && {
              trx_jabatan_instansi_type: 3,
              trx_jabatan_instansi: 129,
            }),

            // Add the new file IDs
            ...fileIdMap,
          };

          // --- 5. CREATE 'trx_jabatan' RECORD ---
          await tx.trx_jabatan.create({
            data: dataJabatan,
          });
        }); // --- End of Transaction ---

        logger.info(`[SUCCESS] Imported record ${record.id} for NIP ${nip}`);
      } catch (e) {
        logger.error(
          `[FAIL] Failed record ${record.id} for NIP ${nip}: ${e.message}`,
        );
        // We log the error and continue to the next record
      }
    }
  }

  logger.info("--- Importer Script Finished ---");
}

// --- RUN THE SCRIPT ---
main()
  .catch((e) => {
    logger.error(`[FATAL] The script encountered a fatal error: ${e.messa}`);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
    logger.info("--- Database disconnected ---");
  });
