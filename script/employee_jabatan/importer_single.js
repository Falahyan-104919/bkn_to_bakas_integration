// 1. IMPORTS
const fsp = require("fs").promises;
const fs = require("fs");
const path = require("path");
const { PrismaClient } = require("@prisma/client");
const logger = require("./logger");

const prisma = new PrismaClient();

// --- CONFIGURATION ---
const STAGING_DATA_DIR = path.join(__dirname, "staging_data");
const STAGING_FILES_DIR = path.join(__dirname, "temp_downloads");
const SUPERADMIN_ID = 1;
const STATUS_SYNC_BKN = 3;

const FINAL_FILE_DESTINATION_BASE = "/home/linux/sinetron-back/assets/upload";

const BKN_DOC_ID_TO_FILE_KEY = {
  872: "SK_JABATAN",
  873: "SK_PELANTIKAN",
};

const LOCAL_FILE_KEY_MAPPING = {
  skJabatan: { fileType: 11, field: "trx_jabatan_file_id" },
  spPelantikan: { fileType: 40, field: "trx_jabatan_file_spp" },
  baJabatan: { fileType: 41, field: "trx_jabatan_file_ba" },
};

// --- HELPER FUNCTIONS ---
function sanitizeString(str) {
  if (!str) return null;
  return str.replace(/[\uFFFD]/g, "");
}

function parseDate(dateString) {
  if (!dateString || typeof dateString !== "string") return null;
  const [dayString, monthString, yearString] = dateString.split("-");
  if (!dayString || !monthString || !yearString || yearString.length !== 4) {
    return null;
  }

  const day = Number.parseInt(dayString, 10);
  const month = Number.parseInt(monthString, 10);
  const year = Number.parseInt(yearString, 10);

  if (!Number.isInteger(day) || !Number.isInteger(month) || !Number.isInteger(year) || month < 1 || month > 12 || day < 1 || day > 31) {
    return null;
  }

  const parsedDate = new Date(year, month - 1, day);

  if (Number.isNaN(parsedDate.getTime()) || parsedDate.getFullYear() !== year || parsedDate.getMonth() !== month - 1 || parsedDate.getDate() !== day) {
    return null;
  }

  return parsedDate;
}

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

const findEselon = async (record) => {
  if (!record.eselonId) return null;
  const eselon = await prisma.ms_eselon.findFirst({
    where: {
      eselon_kode: parseInt(record.eselonId, 10),
    },
  });
  return eselon ? eselon.eselon_id : null;
};

const findOrganizationAndJabatan = async (record) => {
  const { instansiKerjaId, satuanKerjaId, unorId, unorIndukNama, unorNama, namaJabatan, satuanKerjaNama } = record;
  const eselon_id = await findEselon(record);

  const instansiID = await prisma.ms_instansi_pusat.findFirst({
    where: {
      AND: [{ ms_instansi_pusat_instansi_id: instansiKerjaId }, { ms_instansi_pusat_satker_id: satuanKerjaId }],
    },
  });

  const provinsiID = await prisma.ms_provinsi.findFirst({
    where: {
      AND: [{ provinsi_instansi_id: instansiKerjaId }, { provinsi_satker_id: satuanKerjaId }],
    },
  });

  const kabKotID = await prisma.ms_kota.findFirst({
    where: {
      AND: [{ kota_instansi_id: instansiKerjaId }, { kota_satker_id: satuanKerjaId }],
    },
  });

  if (instansiID) {
    return {
      trx_jabatan_instansi_type: 1,
      trx_jabatan_instansi: instansiID.ms_instansi_pusat_id,
      trx_jabatan_jabatan_organization: `${satuanKerjaNama} ${unorIndukNama} ${unorNama}`,
      trx_jabatan_jabatan_eselon: eselon_id,
      trx_jabatan_jabatan_nama: namaJabatan,
    };
  }

  if (provinsiID) {
    return {
      trx_jabatan_instansi_type: 2,
      trx_jabatan_instansi: provinsiID.provinsi_id,
      trx_jabatan_jabatan_organization: `${satuanKerjaNama} ${unorIndukNama} ${unorNama}`,
      trx_jabatan_jabatan_eselon: eselon_id,
      trx_jabatan_jabatan_nama: namaJabatan,
    };
  }

  if (kabKotID) {
    const data = {
      trx_jabatan_instansi_type: 3,
      trx_jabatan_instansi: kabKotID.kota_id,
    };
    const jabatanKode = findJabatanKode(record);
    const organization = await prisma.ms_organization.findFirst({
      where: {
        organization_bkn_id: unorId,
      },
    });
    if (organization) {
      const jabatanID = await prisma.ms_jabatan.findFirst({
        where: {
          jabatan_kode: jabatanKode,
        },
      });
      data.trx_jabatan_jabatan_id = jabatanID ? jabatanID.jabatan_id : null;
      if (!jabatanID && record.jenisJabatan == "4") {
        const newFungsionalUmum = await prisma.ms_jabatan.create({
          data: {
            jabatan_nama: record.jabatanFungsionalUmumNama,
            jabatan_kode: jabatanKode,
            jabatan_create_by: SUPERADMIN_ID,
            jabatan_create_date: new Date(),
            jabatan_tipe: 2,
            jabatan_status: 0,
          },
        });
        data.trx_jabatan_jabatan_id = newFungsionalUmum.jabatan_id;
      }
      data.trx_jabatan_organization_id = organization.organization_id;
      return data;
    } else {
      data.trx_jabatan_jabatan_organization = `${satuanKerjaNama} ${unorIndukNama} ${unorNama}`;
      data.trx_jabatan_jabatan_eselon = eselon_id;
      data.trx_jabatan_jabatan_nama: namaJabatan;
      return data;
    }
  }

  return null;
};

function sanitizeFileName(name) {
  if (!name) return "UNKNOWN";
  return name
    .toUpperCase()
    .replace(/\s+/g, "_")
    .replace(/[^A-Z0-9_]/g, "");
}

function isBlank(value) {
  return value === null || value === undefined || (typeof value === "string" && value.trim() === "");
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
  return record?.nipBaru ?? record?.nip ?? record?.employee_nip ?? record?.employeeNip ?? record?.nipbaru ?? null;
}

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
        logger.warn(`[SKIP] Record ${record.id} (NIP ${nip}) missing jabatan/organization metadata. Skipping.`);
        continue;
      }

      const employee = await prisma.ms_employee.findFirst({
        where: {
          employee_nip: nip,
          employee_status: { notIn: [0] },
        },
      });
      if (!employee) {
        throw new Error(`NIP ${record.nipBaru} not found in local ms_employee`);
      }

      const jabatanKode = findJabatanKode(record);
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
      const organizationAndJabatan = await findOrganizationAndJabatan(record);

      const parsedTmtJabatan = parseDate(record.tmtJabatan);
      if (!parsedTmtJabatan) {
        logger.warn(`[SKIP] Invalid or missing TMT for record ${record.id} (NIP ${nip}).`);
        continue;
      }
      const dataPayload = {
        trx_jabatan_nomor_sk: sanitizeString(record.nomorSk),
        trx_jabatan_tgl_sk: parseDate(record.tanggalSk),
        trx_jabatan_status: STATUS_SYNC_BKN,
        trx_jabatan_jenis_sk: 3,
        trx_jabatan_pejabat_sk: null,
        trx_jabatan_status_jabatan: 1,
        trx_jabatan_file_ba: null,
        trx_jabatan_type: 1,
        ...organizationAndJabatan,
      };

      const fileCreateDataMap = new Map();

      if (record.path && typeof record.path === "object") {
        for (const [docKey, fileInfo] of Object.entries(record.path)) {
          const fileKeyName = BKN_DOC_ID_TO_FILE_KEY[docKey];
          if (!fileKeyName) continue;

          if (!fileInfo || !fileInfo.dok_uri || typeof fileInfo.dok_uri !== "string") {
            logger.warn(`[FILE] Missing or invalid dok_uri for doc ${docKey} on record ${record.id}.`);
            continue;
          }

          const basename = path.basename(fileInfo.dok_uri);
          if (!basename) {
            logger.warn(`[FILE] Unable to resolve filename from dok_uri for doc ${docKey} on record ${record.id}.`);
            continue;
          }

          const safeDownloadedFilename = `${record.id}_${docKey}_${basename}`;
          const sourcePath = path.join(STAGING_FILES_DIR, safeDownloadedFilename);

          if (!fs.existsSync(sourcePath)) {
            logger.warn(`[FILE] File not found in temp_downloads: ${safeDownloadedFilename}`);
            continue;
          }

          const fileExt = path.extname(basename).toLowerCase() || ".pdf";
          const fileExtWithoutDot = fileExt.startsWith(".") ? fileExt.substring(1) : fileExt;

          const jabatanNamaPart = sanitizeFileName(record.namaJabatan);
          const dateString = parseDate(record.tanggalSk) || new Date();
          const datePart = `${String(dateString.getDate()).padStart(2, "0")}${String(dateString.getMonth() + 1).padStart(2, "0")}${String(dateString.getFullYear())}`;
          const fileKeyPart = fileKeyName;

          const newFilename = `${nip}_${fileKeyPart}_${jabatanNamaPart}_${datePart}_${fileExt}`;
          const finalDirPath = path.join(FINAL_FILE_DESTINATION_BASE, nip);
          const finalFilePath = path.join(finalDirPath, newFilename);

          const fileMapping = LOCAL_FILE_KEY_MAPPING[fileKeyName];
          if (!fileMapping) {
            logger.warn(`[FILE] No LOCAL_FILE_KEY_MAPPING entry for key ${fileKeyName} (doc ${docKey}).`);
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
              file_ext: fileExtWithoutDot,
            },
          });

          fileMoveOps.push({ sourcePath, finalFilePath, finalDirPath });
        }
      }

      for (const op of fileMoveOps) {
        await fsp.mkdir(op.finalDirPath, { recursive: true });
        await fsp.copyFile(op.sourcePath, op.finalFilePath);
      }

      let txSuccess = false;
      await prisma.$transaction(async (tx) => {
        const uniqueWhere = {
          trx_jabatan_employee_id_trx_jabatan_tmt: {
            trx_jabatan_employee_id: employee.employee_id,
            trx_jabatan_tmt: parsedTmtJabatan,
          },
        };

        const existingRecord = await tx.trx_jabatan.findUnique({
          where: uniqueWhere,
        });

        let jabatanRecord;

        if (existingRecord && existingRecord.trx_jabatan_status !== STATUS_SYNC_BKN) {
          logger.info(`[SKIP UPDATE] Record for NIP ${nip} / TMT ${record.tmtJabatan} was manually edited locally. Skipping overwrite.`);
          jabatanRecord = existingRecord;
        } else {
          jabatanRecord = await tx.trx_jabatan.upsert({
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
        }

        if (fileCreateDataMap.size === 0) {
          return;
        }

        const fileIdsToLink = {};

        for (const [, fileData] of fileCreateDataMap.entries()) {
          const targetField = fileData.fileMapping.field;
          let fileId;

          if (existingRecord && existingRecord[targetField]) {
            fileId = existingRecord[targetField];
            await tx.trx_employee_file.update({
              where: { file_id: fileId },
              data: {
                file_name: fileData.createData.file_name,
                file_path: fileData.createData.file_path,
                file_size: fileData.createData.file_size,
                file_ext: fileData.createData.file_ext,
                file_status: 1,
              },
            });
            logger.info(`[FILE_DB] Updated existing file record ID ${fileId} for ${targetField}.`);
          } else {
            const newFileRecord = await tx.trx_employee_file.create({
              data: fileData.createData,
            });
            fileId = newFileRecord.file_id;
            fileIdsToLink[targetField] = fileId;
            logger.info(`[FILE_DB] Created new file record ID ${fileId} for ${targetField}.`);
          }
        }

        if (Object.keys(fileIdsToLink).length > 0) {
          await tx.trx_jabatan.update({
            where: {
              trx_jabatan_id: jabatanRecord.trx_jabatan_id,
            },
            data: fileIdsToLink,
          });
        }

        logger.info(`[UPSERT] Upserted record for NIP ${nip} / TMT ${record.tmtJabatan}.`);
      });
      txSuccess = true;

      for (const op of fileMoveOps) {
        if (fs.existsSync(op.sourcePath)) {
          await fsp.unlink(op.sourcePath);
        }
        logger.info(`[FILE_MOVE] Successfully finalized file: ${op.finalFilePath}`);
      }

      logger.info(`[SUCCESS] Processed record ${record.id} for NIP ${nip}`);
    } catch (e) {
      if (!txSuccess) {
        for (const op of fileMoveOps) {
          if (fs.existsSync(op.finalFilePath)) {
            await fsp.unlink(op.finalFilePath).catch(() => {});
            logger.warn(`[ROLLBACK] Removed orphaned file copy: ${op.finalFilePath}`);
          }
        }
      }
      logger.error(`[FAIL] Failed record ${record.id} for NIP ${nip}: ${e.message}`);
      logger.error(e.stack);
    }
  }

  return true;
}

// --- RUN A SINGLE TEST ---
async function runSingleTest() {
  logger.info("--- Starting Single Record Test ---");

  // EDIT THIS TO TEST A SPECIFIC SCENARIO
  const testNip = "198501012010011001";

  const testRecord = [
      {
        "id": "ba970862-d79c-4b53-9315-25d3f2c20a6b",
        "idPns": "7E85A273D894BD8DE050640A3C036B36",
        "nipBaru": "199310122019031001",
        "nipLama": "",
        "jenisJabatan": "1",
        "instansiKerjaId": "A5EB03E23C37F6A0E040640A040252AD",
        "instansiKerjaNama": "Pemerintah Kab. Lampung Barat",
        "satuanKerjaId": "A5EB03E241B3F6A0E040640A040252AD",
        "satuanKerjaNama": "Pemerintah Kab. Lampung Barat",
        "unorId": "8ae48288597fac650159824e03aa2470",
        "unorNama": "SUB BAGIAN UMUM DAN PERENCANAAN DINAS KOMUNIKASI DAN INFORMATIKA",
        "unorIndukId": "8ae48288597fac6501598230aed51447",
        "unorIndukNama": "DINAS KOMUNIKASI DAN INFORMATIKA",
        "eselon": "IV.a",
        "eselonId": "41",
        "jabatanFungsionalId": "",
        "jabatanFungsionalNama": "",
        "jabatanFungsionalUmumId": "",
        "jabatanFungsionalUmumNama": "",
        "tmtJabatan": "29-01-2024",
        "nomorSk": "B/91/KPTS/IV.05/2024",
        "tanggalSk": "29-01-2024",
        "namaUnor": "",
        "namaJabatan": "KEPALA SUB BAGIAN UMUM DAN PERENCANAAN DINAS KOMUNIKASI DAN INFORMATIKA",
        "tmtPelantikan": "01-01-0001",
        "path": {
          "872": {
            "dok_id": "872",
            "dok_nama": "Dok SK Jabatan",
            "dok_uri": "peremajaan/usulan/872_ba970862-d79c-4b53-9315-25d3f2c20a6b.pdf",
            "object": "peremajaan/usulan/872_ba970862-d79c-4b53-9315-25d3f2c20a6b.pdf",
            "slug": "872"
          }
        },
        "jenisPenugasanId": "D",
        "jenisMutasiId": "",
        "subJabatanId": "0",
        "tmtMutasi": "01-01-0001",
        "createdAt": "28-02-2026",
        "updatedAt": "28-02-2026",
        "deletedAt": null
      },
      {
        "id": "8ff6b807-ed3f-11ef-a362-0a580a8009e6",
        "idPns": "7E85A273D894BD8DE050640A3C036B36",
        "nipBaru": "199310122019031001",
        "nipLama": "",
        "jenisJabatan": "4",
        "instansiKerjaId": "A5EB03E23C37F6A0E040640A040252AD",
        "instansiKerjaNama": "Pemerintah Kab. Lampung Barat",
        "satuanKerjaId": "A5EB03E241B3F6A0E040640A040252AD",
        "satuanKerjaNama": "Pemerintah Kab. Lampung Barat",
        "unorId": "8ae48288597fac650159824c6da5236c",
        "unorNama": "BIDANG APLIKASI INFORMATIKA",
        "unorIndukId": "8ae48288597fac6501598230aed51447",
        "unorIndukNama": "DINAS KOMUNIKASI DAN INFORMATIKA",
        "eselon": null,
        "eselonId": null,
        "jabatanFungsionalId": "",
        "jabatanFungsionalNama": "",
        "jabatanFungsionalUmumId": "ff8080813c9550be013cb24870ae6799",
        "jabatanFungsionalUmumNama": "PENGENDALI JARINGAN KOMUNIKASI",
        "tmtJabatan": "20-03-2021",
        "nomorSk": "B/9/KPTS/IV.04/2019",
        "tanggalSk": "15-02-2021",
        "namaUnor": "BIDANG APLIKASI INFORMATIKA",
        "namaJabatan": "PENGENDALI JARINGAN KOMUNIKASI",
        "tmtPelantikan": "20-03-2021",
        "path": {
          "": {
            "dok_id": "",
            "dok_nama": "",
            "dok_uri": "",
            "object": "",
            "slug": ""
          }
        },
        "jenisPenugasanId": "",
        "jenisMutasiId": "",
        "subJabatanId": "",
        "tmtMutasi": "",
        "createdAt": "17-02-2025",
        "updatedAt": "17-03-2025",
        "deletedAt": null
      },
      {
        "id": "8ae483a568cb79a20168ff46682e16a7",
        "idPns": "7E85A273D894BD8DE050640A3C036B36",
        "nipBaru": "199310122019031001",
        "nipLama": "",
        "jenisJabatan": "4",
        "instansiKerjaId": "A5EB03E23C37F6A0E040640A040252AD",
        "instansiKerjaNama": "Pemerintah Kab. Lampung Barat",
        "satuanKerjaId": "A5EB03E241B3F6A0E040640A040252AD",
        "satuanKerjaNama": "Pemerintah Kab. Lampung Barat",
        "unorId": "8ae48288597fac650159824c6da5236c",
        "unorNama": "BIDANG APLIKASI INFORMATIKA",
        "unorIndukId": "8ae48288597fac6501598230aed51447",
        "unorIndukNama": "DINAS KOMUNIKASI DAN INFORMATIKA",
        "eselon": null,
        "eselonId": null,
        "jabatanFungsionalId": "",
        "jabatanFungsionalNama": "",
        "jabatanFungsionalUmumId": "ff8080813c9550be013cb24870ae6799",
        "jabatanFungsionalUmumNama": "PENGENDALI JARINGAN KOMUNIKASI",
        "tmtJabatan": "14-02-2019",
        "nomorSk": "AG-21801000038",
        "tanggalSk": "14-02-2019",
        "namaUnor": "DINAS KOMUNIKASI DAN INFORMATIKA",
        "namaJabatan": "PENGENDALI JARINGAN KOMUNIKASI",
        "tmtPelantikan": "",
        "path": {
          "872": {
            "dok_id": "872",
            "dok_nama": "Dok SK Jabatan",
            "dok_uri": "peremajaan/usulan/872_0dbdedc1-f218-4f0d-b2d8-e50ce8ff766b.pdf",
            "object": "peremajaan/usulan/872_0dbdedc1-f218-4f0d-b2d8-e50ce8ff766b.pdf",
            "slug": "872"
          }
        },
        "jenisPenugasanId": "",
        "jenisMutasiId": "",
        "subJabatanId": "",
        "tmtMutasi": "",
        "createdAt": null,
        "updatedAt": "24-08-2023",
        "deletedAt": null
      }
    ];

  await processRecordsForNip(testNip, testRecord);

  logger.info("--- Single Record Test Finished ---");
}

if (require.main === module) {
  runSingleTest()
    .catch((e) => {
      logger.error(`[FATAL] Test script error: ${e.message}`);
    })
    .finally(async () => {
      await prisma.$disconnect();
    });
}
