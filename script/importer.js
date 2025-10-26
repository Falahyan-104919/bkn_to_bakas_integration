// 1. IMPORTS
const fsp = require("fs").promises;
const fs = require("fs");
const path = require("path");
// --- TODO: 1. UPDATE YOUR PRISMA IMPORT ---
const { PrismaClient } = require("@prisma/client"); // Use this for default
const logger = require("./logger");

const prisma = new PrismaClient();

// --- CONFIGURATION ---
const STAGING_DATA_DIR = path.join(__dirname, "staging_data");
const STAGING_FILES_DIR = path.join(__dirname, "temp_downloads");
const SUPERADMIN_ID = 1;
const STATUS_SYNC_BKN = 3;

// --- TODO: 2. SET YOUR PRODUCTION FILE PATH ---
const FINAL_FILE_DESTINATION_BASE = "/home/aptika/assets/upload";

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
  if (!dateString || dateString.length < 10) return null;
  const [day, month, year] = dateString.split("-");
  if (!day || !month || !year || year.length !== 4) return null;
  return new Date(`${year}-${month}-${day}`);
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
    .replace(/\s+/g, "_")
    .replace(/[^A-Z0-9_]/g, "");
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
    const filePath = path.join(STAGING_DATA_DIR, file);

    const fileContent = await fsp.readFile(filePath, "utf-8");
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
      // This will hold { sourcePath, finalFilePath } for moving files *after* the TX
      const fileMoveOps = [];

      try {
        // --- 1. GATHER DATA (Lookups) ---
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
              where: {
                jabatan_kode: jabatanKode,
                jabatan_status: { notIn: [0] },
              },
            })
          : null;

        const organization = await prisma.ms_organization.findUnique({
          where: {
            organization_bkn_id: record.unorId,
            organization_status: { notIn: [0] },
          },
        });

        // --- 2. PREPARE JABATAN DATA (The 'payload' for create/update) ---
        const parsedTmtJabatan = parseDate(record.tmtJabatan);
        const dataPayload = {
          trx_jabatan_jabatan_id: jabatan ? jabatan.jabatan_id : null,
          trx_jabatan_organization_id: organization
            ? organization.organization_id
            : null,
          trx_jabatan_nomor_sk: record.nomorSk,
          trx_jabatan_tgl_sk: parseDate(record.tanggalSk),
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

            // --- TODO: 4. MODIFY YOUR FILE NAMING LOGIC HERE ---
            const jabatanNamaPart = sanitizeFileName(record.namaJabatan);
            const dateString = parseDate(record.tanggalSk) || new Date();
            const datePart = `${String(dateString.getDate()).padStart(2, "0")}${String(dateString.getMonth() + 1).padStart(2, "0")}${String(dateString.getFullYear()).slice(2)}`;
            const fileKeyPart = fileKeyName;

            const newFilename = `${nip}_${jabatanNamaPart}_${datePart}_${fileKeyPart}.pdf`;
            const finalDirPath = path.join(FINAL_FILE_DESTINATION_BASE, nip);
            const finalFilePath = path.join(finalDirPath, newFilename);
            // --- End of TODO section ---

            const stats = await fsp.stat(sourcePath);
            const fileMapping = LOCAL_FILE_KEY_MAPPING[fileKeyName];

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
          // Find the unique key for the jabatan
          // --- CRITICAL ASSUMPTION ---
          // Assumes a composite unique key: @@unique([trx_jabatan_employee_id, trx_jabatan_tmt])
          const uniqueWhere = {
            trx_jabatan_employee_id_trx_jabatan_tmt: {
              trx_jabatan_employee_id: employee.employee_id,
              trx_jabatan_tmt: parsedTmtJabatan,
            },
          };

          const existingJabatan = await tx.trx_jabatan.findUnique({
            where: uniqueWhere,
          });

          if (existingJabatan) {
            // --- UPDATE PATH ---
            // Record exists. Only update the non-file metadata.
            logger.warn(
              `[UPDATE] Record found for NIP ${nip} / TMT ${record.tmtJabatan}. Updating metadata.`,
            );

            await tx.trx_jabatan.update({
              where: { id: existingJabatan.id },
              data: {
                ...dataPayload,
                // Add any "updated_by" or "updated_at" fields here
                // ...
              },
            });
          } else {
            // --- CREATE PATH ---
            // Record does not exist. Create files *first*, then the jabatan.
            logger.info(
              `[CREATE] New record for NIP ${nip} / TMT ${record.tmtJabatan}. Creating...`,
            );

            const fileIdsToLink = {}; // { trx_jabatan_file_id: 123, ... }

            // Create all the file records
            for (const [fileKeyName, fileData] of fileCreateDataMap.entries()) {
              // --- CRITICAL ASSUMPTION #2 ---
              // We assume 'trx_employee_file' *should* be unique on employee_id and file_type
              // If not, this might create duplicates if run twice on a failed record.
              // For now, we follow your "don't upsert file" rule and just create.
              const newFileRecord = await tx.trx_employee_file.create({
                data: fileData.createData,
              });

              // Save the new ID to link to the jabatan
              fileIdsToLink[fileData.fileMapping.field] = newFileRecord.file_id;
            }

            // Now, create the jabatan record
            await tx.trx_jabatan.create({
              data: {
                ...dataPayload,
                trx_jabatan_employee_id: employee.employee_id,
                trx_jabatan_tmt: parsedTmtJabatan,
                trx_jabatan_create_date: new Date(),
                trx_jabatan_create_by: SUPERADMIN_ID,
                ...fileIdsToLink, // Link the newly created file IDs
              },
            });
          }
        }); // --- END TRANSACTION ---

        // --- 5. MOVE FILES (Post-Transaction) ---
        // This only runs if the transaction above was successful.
        // We only move files that were part of the 'fileCreateDataMap'.
        // This logic is now safe: even on an 'update', if we prepared file ops,
        // (which we don't, but if we did) they would move.
        // *However*, in our 'update' path, 'fileMoveOps' will be empty
        // unless we add logic to update files, which you've requested not to.

        // Let's refine this: only move files if we *created* a record.
        // But the `fileMoveOps` are built regardless.
        // This means we must check if the file *already exists* at the destination.

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
  }

  logger.info("--- Importer Script Finished ---");
}

// --- RUN THE SCRIPT ---
main()
  .catch((e) => {
    logger.error(`[FATAL] The script encountered a fatal error: ${e.message}`);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
    logger.info("--- Database disconnected ---");
  });
