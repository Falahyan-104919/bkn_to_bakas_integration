const fsp = require("fs").promises;
const fs = require("fs");
const path = require("path");

const FINAL_FILE_DESTINATION_BASE = "/home/linux/sinetron-back/assets/upload";

const { PrismaClient } = require("@prisma/client");
const logger = require("../logger");

const prisma = new PrismaClient();

const STAGING_DATA_DIR = path.resolve(__dirname, "staging_golongan");
const STAGING_FILES_DIR = path.join(__dirname, "temp_downloads");
const SUPERADMIN_ID = 1;
const BKN_DOC_ID_TO_FILE_KEY = {
  858: "SK_PANGKAT",
  50: "SK_PETIKAN_PPK",
};
const LOCAL_FILE_KEY_MAPPING = {
  SK_PANGKAT: { fileType: 12, field: "pangkat_file_id" },
  SK_PETIKAN_PPK: { fileType: 1, field: "pangkat_file_id" },
};

const toNullIfEmpty = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length === 0 ? null : trimmed;
  }
  return value;
};

const toInt = (value) => {
  const cleaned = toNullIfEmpty(value);
  if (cleaned === null) return null;
  const numeric = Number(cleaned);
  return Number.isFinite(numeric) ? numeric : null;
};

const parseDate = (value) => {
  const cleaned = toNullIfEmpty(value);
  if (!cleaned || cleaned === "01-01-0001") return null;
  const formatted = cleaned.split("T")[0];
  const [day, month, year] = formatted.split("-");
  if (!day || !month || !year) return null;
  const isoDate = `${year.padStart(4, "0")}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  const parsed = new Date(isoDate);
  console.log("parsed", parsed);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const buildEmployeeData = async (profile) => {
  const data = {
    pangkat_tanggal_tmt_golongan: parseDate(profile.tmtGolongan),
    pangkat_nomor_sk: profile.skNomor,
    pangkat_tanggal_sk: parseDate(profile.skTanggal),
    pangkat_status: 1,
    pangkat_jenis_sk: 4,
    pangkat_nomor_sk_bkn: profile.noPertekBkn,
    pangkat_tanggal_sk_bkn: parseDate(profile.tglPertekBkn),
    pangkat_masa_kerja_tahun: toInt(profile.masaKerjaGolonganTahun),
    pangkat_masa_kerja_bulan: toInt(profile.masaKerjaGolonganBulan),
    pangkat_kredit_utama: parseFloat(profile.jumlahKreditUtama),
    pangkat_kredit_tambahan: parseFloat(profile.jumlahKreditTambahan),
    pangkat_bkn_id: profile.id,
  };

  return Object.fromEntries(Object.entries(data).filter(([, value]) => value !== undefined));
};

const persistProfile = async (profile) => {
  const baseData = await buildEmployeeData(profile);
  const now = new Date();
  const nip = profile.nipBaru;

  logger.info(`[INFO RECORD] Processing record ${profile.id} for NIP: ${nip}`);

  const fileMoveOps = [];
  const fileCreateDataMap = new Map();

  if (profile.path && typeof profile.path === "object") {
    for (const [docKey, fileInfo] of Object.entries(profile.path)) {
      const fileKeyName = BKN_DOC_ID_TO_FILE_KEY[docKey];
      if (!fileKeyName) continue;

      if (!fileInfo || !fileInfo.dok_uri) continue;

      const basename = path.basename(fileInfo.dok_uri);
      const safeDownloadedFilename = `${profile.id}_${docKey}_${basename}`;
      const sourcePath = path.join(STAGING_FILES_DIR, safeDownloadedFilename);

      if (!fs.existsSync(sourcePath)) {
        logger.warn(`[FILE] File not found in temp_downloads: ${safeDownloadedFilename}`);
        continue;
      }

      const golonganString = profile.golongan || "";
      const newFilename = `${nip}_${fileKeyName}_${golonganString.replaceAll("/", "")}.pdf`;
      const finalDirPath = path.join(FINAL_FILE_DESTINATION_BASE, nip);
      const finalFilePath = path.join(finalDirPath, newFilename);

      const fileMapping = LOCAL_FILE_KEY_MAPPING[fileKeyName];
      if (!fileMapping) continue;

      const stats = await fsp.stat(sourcePath);
      fileCreateDataMap.set(fileKeyName, {
        fileMapping: fileMapping,
        createData: {
          file_name: newFilename,
          file_type: fileMapping.fileType,
          file_path: finalFilePath,
          file_status: 1,
          file_create_by: SUPERADMIN_ID,
          file_create_date: now,
          file_size: stats.size,
          file_ext: "pdf",
        },
      });

      fileMoveOps.push({ sourcePath, finalFilePath, finalDirPath });
    }
  }

  return prisma.$transaction(async (tx) => {
    const ms_employee = await tx.ms_employee.findUnique({
      where: {
        employee_nip: profile.nipBaru,
      },
    });
    if (!ms_employee) {
      logger.warn(`[SKIP] NIP ${profile.nipBaru} not found in ms_employee`);
      return;
    }
    const employee_id = ms_employee.employee_id;

    const ms_golongan = await tx.ms_golongan.findUnique({
      where: {
        golongan_kode: toInt(profile.golonganId),
      },
    });
    if (!ms_golongan) {
      logger.warn(`[SKIP] Golongan ${profile.golonganId} not found in ms_golongan`);
      return;
    }
    const golongan_id = ms_golongan.golongan_id;

    const fileIdsToLink = {};
    for (const [, fileData] of fileCreateDataMap.entries()) {
      const newFileRecord = await tx.trx_employee_file.create({
        data: {
          ...fileData.createData,
          file_employee_id: employee_id,
        },
      });
      fileIdsToLink[fileData.fileMapping.field] = newFileRecord.file_id;
    }

    const employeeP3KGolonganRecord = await tx.trx_pangkat.upsert({
      where: {
        pangkat_employee_id_pangkat_tanggal_tmt_golongan: {
          pangkat_employee_id: employee_id,
          pangkat_tanggal_tmt_golongan: baseData.pangkat_tanggal_tmt_golongan,
        },
      },
      update: {
        pangkat_employee_id: employee_id,
        pangkat_golongan_id: golongan_id,
        ...baseData,
        ...fileIdsToLink,
        pangkat_create_by: SUPERADMIN_ID,
        pangkat_create_date: now,
      },
      create: {
        pangkat_employee_id: employee_id,
        pangkat_golongan_id: golongan_id,
        ...baseData,
        ...fileIdsToLink,
        pangkat_create_by: SUPERADMIN_ID,
        pangkat_create_date: now,
      },
    });

    for (const op of fileMoveOps) {
      await fsp.mkdir(op.finalDirPath, { recursive: true });
      if (fs.existsSync(op.finalFilePath)) {
        await fsp.unlink(op.finalFilePath);
      }
      await fsp.rename(op.sourcePath, op.finalFilePath);
      logger.info(`[FILE_MOVE] Moved file to: ${op.finalFilePath}`);
    }

    return { employeeP3KGolonganRecord };
  });
};

const importAllProfiles = async () => {
  const dirEntries = await fsp.readdir(STAGING_DATA_DIR, {
    withFileTypes: true,
  });
  const jsonFiles = dirEntries.filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(".json"));

  if (jsonFiles.length === 0) {
    logger.warn(`[IMPORT] No JSON payloads found in ${STAGING_DATA_DIR}`);
    return;
  }

  for (const file of jsonFiles) {
    const filePath = path.join(STAGING_DATA_DIR, file.name);
    try {
      const raw = await fsp.readFile(filePath, "utf-8");
      const payload = JSON.parse(raw);

      if (!payload || payload.code !== 1 || !payload.data) {
        logger.warn(`[IMPORT] Skipping ${file.name}: invalid payload structure`);
        continue;
      }

      for (const profile of payload.data) {
        await persistProfile(profile);
      }
      logger.info(`[IMPORT] Imported records for NIP ${payload.data[0].nipBaru} from ${file.name}`);
    } catch (error) {
      logger.error(`[IMPORT] Failed to process ${file.name}: ${error.message}`);
    }
  }
};

importAllProfiles()
  .catch((error) => {
    logger.error(`[IMPORT Golongan Records] Unexpected failure: ${error.message}`);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
