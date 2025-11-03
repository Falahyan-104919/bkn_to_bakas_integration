const fsp = require("fs").promises;
const path = require("path");

const { PrismaClient } = require("@prisma/client");
const logger = require("../logger");

const prisma = new PrismaClient();

const STAGING_DATA_DIR = path.resolve(__dirname, "staging_employee_golongan");
const SUPERADMIN_ID = 1;

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
  };

  return Object.fromEntries(
    Object.entries(data).filter(([, value]) => value !== undefined),
  );
};

const persistProfile = async (profile) => {
  const baseData = await buildEmployeeData(profile);
  const now = new Date();

  logger.info(`[INFO RECORD] ${JSON.stringify(profile)}`);

  return prisma.$transaction(async (tx) => {
    const { employee_id } = await tx.ms_employee.findUnique({
      where: {
        employee_nip: profile.nipBaru,
      },
    });
    const { golongan_id } = await tx.ms_golongan.findUnique({
      where: {
        golongan_kode: profile.golonganId,
      },
    });
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
        pangkat_create_by: SUPERADMIN_ID,
        pangkat_create_date: now,
      },
      create: {
        pangkat_employee_id: employee_id,
        pangkat_golongan_id: golongan_id,
        ...baseData,
        pangkat_create_by: SUPERADMIN_ID,
        pangkat_create_date: now,
      },
    });

    return { employeeP3KGolonganRecord };
  });
};

const importAllProfiles = async () => {
  const dirEntries = await fsp.readdir(STAGING_DATA_DIR, {
    withFileTypes: true,
  });
  const jsonFiles = dirEntries.filter(
    (entry) => entry.isFile() && entry.name.toLowerCase().endsWith(".json"),
  );

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
        logger.warn(
          `[IMPORT] Skipping ${file.name}: invalid payload structure`,
        );
        continue;
      }

      await persistProfile(payload.data[0]);
      logger.info(
        `[IMPORT] Imported ${payload.data.nipBaru} from ${file.name}`,
      );
    } catch (error) {
      logger.error(`[IMPORT] Failed to process ${file.name}: ${error.message}`);
    }
  }
};

importAllProfiles()
  .catch((error) => {
    logger.error(
      `[IMPORT P3K Golongan Records] Unexpected failure: ${error.message}`,
    );
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
