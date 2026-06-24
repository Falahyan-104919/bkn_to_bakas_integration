// 1. IMPORTS
const fsp = require("fs").promises;
const fs = require("fs");
const path = require("path");
const { PrismaClient } = require("@prisma/client");
const logger = require("../logger");

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
      data.trx_jabatan_jabatan_nama = jabatanFungsionalNama || jabatanFungsionalUmumNama || namaJabatan;
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
    let txSuccess = false;
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
      id: "37d52279-bf42-4218-bf32-dda831407adf",
      idPns: "A8ACA78EEB8E3912E040640A040269BB",
      nipBaru: "197310111999031004",
      nipLama: "710031129",
      jenisJabatan: "1",
      instansiKerjaId: "A5EB03E23C37F6A0E040640A040252AD",
      instansiKerjaNama: "Pemerintah Kab. Lampung Barat",
      satuanKerjaId: "A5EB03E241B3F6A0E040640A040252AD",
      satuanKerjaNama: "Pemerintah Kab. Lampung Barat",
      unorId: "8ae4828659703898015972cfc0db358b",
      unorNama: "DINAS LINGKUNGAN HIDUP",
      unorIndukId: "A8ACA73D13603912E040640A040269BB",
      unorIndukNama: "PEMERINTAH KABUPATEN LAMPUNG BARAT                                    ",
      eselon: "II.b",
      eselonId: "22",
      jabatanFungsionalId: "",
      jabatanFungsionalNama: "",
      jabatanFungsionalUmumId: "",
      jabatanFungsionalUmumNama: "",
      tmtJabatan: "01-12-2025",
      nomorSk: "B/140/KPTS/IV.05/2025",
      tanggalSk: "02-10-2025",
      namaUnor: "",
      namaJabatan: "KEPALA DINAS LINGKUNGAN HIDUP",
      tmtPelantikan: "02-10-2025",
      path: {
        872: {
          dok_id: "872",
          dok_nama: "Dok SK Jabatan",
          dok_uri: "peremajaan/usulan/872_37d52279-bf42-4218-bf32-dda831407adf.pdf",
          object: "peremajaan/usulan/872_37d52279-bf42-4218-bf32-dda831407adf.pdf",
          slug: "872",
        },
      },
      jenisPenugasanId: "D",
      jenisMutasiId: "",
      subJabatanId: "0",
      tmtMutasi: "01-01-0001",
      createdAt: "31-12-2025",
      updatedAt: "31-12-2025",
      deletedAt: null,
    },
    {
      id: "5a6508f4-2982-11f0-967a-0a580a800952",
      idPns: "A8ACA78EEB8E3912E040640A040269BB",
      nipBaru: "197310111999031004",
      nipLama: "710031129",
      jenisJabatan: "1",
      instansiKerjaId: "0c771fccefd7493aa4f6ef70450f026e",
      instansiKerjaNama: "Kementerian Kehutanan",
      satuanKerjaId: "0cf81543-6f89-4e21-910c-d86ea9a85118",
      satuanKerjaNama: "Kementerian Kehutanan",
      unorId: "f3249798-fbc5-4bd9-ac10-cc45ae826d27",
      unorNama: "Bidang Konservasi Sumber Daya Alam Wilayah I",
      unorIndukId: "0c9671c5-e288-4789-9746-270ee7059e6a",
      unorIndukNama: "Balai Besar Konservasi Sumber Daya Alam Riau",
      eselon: "III.b",
      eselonId: "32",
      jabatanFungsionalId: "",
      jabatanFungsionalNama: "",
      jabatanFungsionalUmumId: null,
      jabatanFungsionalUmumNama: null,
      tmtJabatan: "17-04-2025",
      nomorSk: "NOMOR 159 TAHUN 2025",
      tanggalSk: "17-04-2025",
      namaUnor: "Bidang Konservasi Sumber Daya Alam Wilayah I",
      namaJabatan: "Kepala Bidang Konservasi Sumber Daya Alam Wilayah I",
      tmtPelantikan: "21-04-2025",
      path: {
        872: {
          dok_id: "872",
          dok_nama: "Dok SK Jabatan",
          dok_uri: "peremajaan/usulan/A8ACA7F4BE133912E040640A040269BB_20250506_023817_197310111999031004_1182883.pdf",
          object: "peremajaan/usulan/A8ACA7F4BE133912E040640A040269BB_20250506_023817_197310111999031004_1182883.pdf",
          slug: "872",
        },
      },
      jenisPenugasanId: "",
      jenisMutasiId: "",
      subJabatanId: "",
      tmtMutasi: "",
      createdAt: "05-05-2025",
      updatedAt: "06-05-2025",
      deletedAt: null,
    },
    {
      id: "a9ea9f20-d4a8-11ee-904e-0a580a81052c",
      idPns: "A8ACA78EEB8E3912E040640A040269BB",
      nipBaru: "197310111999031004",
      nipLama: "710031129",
      jenisJabatan: "1",
      instansiKerjaId: "A5EB03E23AFBF6A0E040640A040252AD",
      instansiKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      satuanKerjaId: "A5EB03E2434AF6A0E040640A040252AD",
      satuanKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan Wilayah Kanreg Banjarmasin",
      unorId: "8ae483a67e8a5eb4017e95312ab11e53",
      unorNama: "Bidang Konservasi Sumber Daya Alam Wilayah I",
      unorIndukId: "8ae483a67e8a5eb4017e95312ab11e4b",
      unorIndukNama: "Balai Besar Konservasi Sumber Daya Alam Riau",
      eselon: "III.b",
      eselonId: "32",
      jabatanFungsionalId: "",
      jabatanFungsionalNama: "",
      jabatanFungsionalUmumId: null,
      jabatanFungsionalUmumNama: null,
      tmtJabatan: "23-02-2024",
      nomorSk: "SK.209/MENLHK/SETJEN/PEG.2/2/2024",
      tanggalSk: "22-02-2024",
      namaUnor: "Bidang Konservasi Sumber Daya Alam Wilayah I",
      namaJabatan: "Kepala Bidang Konservasi Sumber Daya Alam Wilayah I",
      tmtPelantikan: "23-02-2024",
      path: {
        872: {
          dok_id: "872",
          dok_nama: "Dok SK Jabatan",
          dok_uri: "peremajaan/usulan/000000006e5f1079016e5f4de92932f4_20240226_131534_197310111999031004_1156017.pdf",
          object: "peremajaan/usulan/000000006e5f1079016e5f4de92932f4_20240226_131534_197310111999031004_1156017.pdf",
          slug: "872",
        },
      },
      jenisPenugasanId: "",
      jenisMutasiId: "",
      subJabatanId: "",
      tmtMutasi: "",
      createdAt: "26-02-2024",
      updatedAt: null,
      deletedAt: null,
    },
    {
      id: "8ae483c584a208910184a79a75a963e7",
      idPns: "A8ACA78EEB8E3912E040640A040269BB",
      nipBaru: "197310111999031004",
      nipLama: "710031129",
      jenisJabatan: "1",
      instansiKerjaId: "A5EB03E23AFBF6A0E040640A040252AD",
      instansiKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      satuanKerjaId: "A5EB03E24643F6A0E040640A040252AD",
      satuanKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan Wilayah Kanreg Jakarta",
      unorId: "8ae483a67e8a5eb4017e95312ad31fe7",
      unorNama: "Bidang Pengelolaan Taman Nasional Wilayah II",
      unorIndukId: "8ae483a67e8a5eb4017e95312ad21fdc",
      unorIndukNama: "Balai Besar Taman Nasional Bukit Barisan Selatan",
      eselon: "III.b",
      eselonId: "32",
      jabatanFungsionalId: "",
      jabatanFungsionalNama: "",
      jabatanFungsionalUmumId: "",
      jabatanFungsionalUmumNama: "",
      tmtJabatan: "01-01-2022",
      nomorSk: "SK TEMP",
      tanggalSk: "01-01-2022",
      namaUnor: "Bidang Pengelolaan Taman Nasional Wilayah II",
      namaJabatan: "Kepala Bidang Pengelolaan Taman Nasional Wilayah II",
      tmtPelantikan: "01-01-2022",
      path: null,
      jenisPenugasanId: "",
      jenisMutasiId: "",
      subJabatanId: "",
      tmtMutasi: "",
      createdAt: "24-11-2022",
      updatedAt: "24-11-2022",
      deletedAt: null,
    },
    {
      id: "8ae482a750ed40690150f3db82cb1daf",
      idPns: "A8ACA78EEB8E3912E040640A040269BB",
      nipBaru: "197310111999031004",
      nipLama: "710031129",
      jenisJabatan: "1",
      instansiKerjaId: "A5EB03E23AFBF6A0E040640A040252AD",
      instansiKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      satuanKerjaId: "A5EB03E240E6F6A0E040640A040252AD",
      satuanKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      unorId: "1C0062B1CDDD1551E050640A15024166",
      unorNama: "Bidang Pengelolaan TN. Wilayah II",
      unorIndukId: "1C0062B1CCFF1551E050640A15024166",
      unorIndukNama: "Balai Besar Taman Nasional Bukit Barisan Selatan",
      eselon: "III.a",
      eselonId: "31",
      jabatanFungsionalId: "",
      jabatanFungsionalNama: "",
      jabatanFungsionalUmumId: null,
      jabatanFungsionalUmumNama: null,
      tmtJabatan: "13-09-2017",
      nomorSk: "SK.495/MENLHK/SETJEN/PEG.2/9/2017 ",
      tanggalSk: "13-09-2017",
      namaUnor: "BIDANG PENGELOLAAN TN. WILAYAH II",
      namaJabatan: "Kepala Bidang Pengelolaan TN. Wilayah II",
      tmtPelantikan: "13-09-2017",
      path: null,
      jenisPenugasanId: "",
      jenisMutasiId: "",
      subJabatanId: "",
      tmtMutasi: "",
      createdAt: null,
      updatedAt: null,
      deletedAt: null,
    },
    {
      id: "8ae4828747584cae014760f2069a6577",
      idPns: "A8ACA78EEB8E3912E040640A040269BB",
      nipBaru: "197310111999031004",
      nipLama: "710031129",
      jenisJabatan: "1",
      instansiKerjaId: "A5EB03E23AFBF6A0E040640A040252AD",
      instansiKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      satuanKerjaId: "A5EB03E240E6F6A0E040640A040252AD",
      satuanKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      unorId: "A8ACA739F6913912E040640A040269BB",
      unorNama: "BAGIAN TATA USAHA",
      unorIndukId: "",
      unorIndukNama: "",
      eselon: "III.b",
      eselonId: "32",
      jabatanFungsionalId: "",
      jabatanFungsionalNama: "",
      jabatanFungsionalUmumId: null,
      jabatanFungsionalUmumNama: null,
      tmtJabatan: "11-03-2016",
      nomorSk: "4654/Menhut-II/Peg/2016",
      tanggalSk: "18-06-2016",
      namaUnor: "BAGIAN TATA USAHA",
      namaJabatan: "KEPALA BAGIAN TATA USAHA",
      tmtPelantikan: "25-06-2016",
      path: null,
      jenisPenugasanId: "",
      jenisMutasiId: "",
      subJabatanId: "",
      tmtMutasi: "",
      createdAt: null,
      updatedAt: null,
      deletedAt: null,
    },
    {
      id: "A8ACA8BC29793912E040640A040269BB",
      idPns: "A8ACA78EEB8E3912E040640A040269BB",
      nipBaru: "197310111999031004",
      nipLama: "710031129",
      jenisJabatan: "1",
      instansiKerjaId: "A5EB03E23AFBF6A0E040640A040252AD",
      instansiKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      satuanKerjaId: "A5EB03E240E6F6A0E040640A040252AD",
      satuanKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      unorId: "AA23F87B5D0FAAA0E040640A02020CE2",
      unorNama: "SEKSI PENGELOLAAN TAMAN NASIONAL WILAYAH III",
      unorIndukId: "",
      unorIndukNama: "",
      eselon: "IV.a",
      eselonId: "41",
      jabatanFungsionalId: "",
      jabatanFungsionalNama: "",
      jabatanFungsionalUmumId: null,
      jabatanFungsionalUmumNama: null,
      tmtJabatan: "17-02-2011",
      nomorSk: "336",
      tanggalSk: "17-02-2011",
      namaUnor: "SEKSI PENGELOLAAN TAMAN NASIONAL WILAYAH III",
      namaJabatan: "KEPALA SEKSI PENGELOLAAN TAMAN NASIONAL WILAYAH III",
      tmtPelantikan: "17-02-2011",
      path: null,
      jenisPenugasanId: "",
      jenisMutasiId: "",
      subJabatanId: "",
      tmtMutasi: "",
      createdAt: null,
      updatedAt: null,
      deletedAt: null,
    },
    {
      id: "8ae482a65086343201508dc3abe229dd",
      idPns: "A8ACA78EEB8E3912E040640A040269BB",
      nipBaru: "197310111999031004",
      nipLama: "710031129",
      jenisJabatan: "1",
      instansiKerjaId: "A5EB03E23AFBF6A0E040640A040252AD",
      instansiKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      satuanKerjaId: "A5EB03E240E6F6A0E040640A040252AD",
      satuanKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      unorId: "1C22CB526E2C56AAE050640A150226F0",
      unorNama: "Seksi Pengelolaan TN. Wilayah II",
      unorIndukId: "1C0062B1CCFF1551E050640A15024166",
      unorIndukNama: "Balai Besar Taman Nasional Bukit Barisan Selatan",
      eselon: null,
      eselonId: null,
      jabatanFungsionalId: "",
      jabatanFungsionalNama: "",
      jabatanFungsionalUmumId: null,
      jabatanFungsionalUmumNama: null,
      tmtJabatan: "15-07-2008",
      nomorSk: "SK. 3787/Menhut-II/Peg/2008",
      tanggalSk: "15-07-2008",
      namaUnor: "Seksi Pengelolaan TN. Wilayah II",
      namaJabatan: "Kepala Seksi Pengelolaan TN. Wilayah II",
      tmtPelantikan: "",
      path: null,
      jenisPenugasanId: "",
      jenisMutasiId: "",
      subJabatanId: "",
      tmtMutasi: "",
      createdAt: null,
      updatedAt: null,
      deletedAt: null,
    },
    {
      id: "A8ACA8BC297A3912E040640A040269BB",
      idPns: "A8ACA78EEB8E3912E040640A040269BB",
      nipBaru: "197310111999031004",
      nipLama: "710031129",
      jenisJabatan: "2",
      instansiKerjaId: "A5EB03E23AFBF6A0E040640A040252AD",
      instansiKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      satuanKerjaId: "A5EB03E240E6F6A0E040640A040252AD",
      satuanKerjaNama: "Kementerian Lingkungan Hidup dan Kehutanan",
      unorId: "AA23F87B60C4AAA0E040640A02020CE2",
      unorNama: "BALAI BESAR TAMAN NASIONAL BUKIT BARISAN SELATAN",
      unorIndukId: "A8ACA739F70F3912E040640A040269BB",
      unorIndukNama: "DIREKTORAT JENDERAL PERLINDUNGAN HUTAN DAN KONSERVASI ALAM",
      eselon: null,
      eselonId: null,
      jabatanFungsionalId: "A5EB03E2406FF6A0E040640A040252AD",
      jabatanFungsionalNama: "Polisi Kehutanan Penyelia",
      jabatanFungsionalUmumId: null,
      jabatanFungsionalUmumNama: null,
      tmtJabatan: "01-10-2000",
      nomorSk: "1",
      tanggalSk: "01-10-2000",
      namaUnor: "BALAI BESAR TAMAN NASIONAL BUKIT BARISAN SELATAN",
      namaJabatan: "AJUN JAGAWANA MADYA",
      tmtPelantikan: "",
      path: null,
      jenisPenugasanId: "",
      jenisMutasiId: "",
      subJabatanId: "",
      tmtMutasi: "",
      createdAt: null,
      updatedAt: null,
      deletedAt: null,
    },
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
