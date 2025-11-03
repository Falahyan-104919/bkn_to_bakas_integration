const fsp = require("fs").promises;
const path = require("path");

const { PrismaClient } = require("@prisma/client");
const logger = require("../logger");

const prisma = new PrismaClient();

const STAGING_DATA_DIR = path.resolve(__dirname, "staging_employee");
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
  const [day, month, year] = cleaned.split("-");
  if (!day || !month || !year) return null;
  const isoDate = `${year.padStart(4, "0")}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  const parsed = new Date(isoDate);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const buildEmployeeData = async (profile) => {
  const data = {
    pangkat_tanggal_tmt_golongan: parseDate(profile.tmtPns),
  };

  return Object.fromEntries(
    Object.entries(data).filter(([, value]) => value !== undefined),
  );
};

const persistProfile = async (profile) => {
  const baseData = await buildEmployeeData(profile);
  const now = new Date();

  return prisma.$transaction(async (tx) => {
    const { employee_id } = await tx.ms_employee.findFirst({
      where: {
        employee_nip: baseData.nipBaru,
      },
    });
    const employeeP3KRecord = await tx.trx_employee_pppk.upsert({
      where: {
        pppk_employee_id_pppk_tmt_start: {
          pppk_employee_id: employee_id,
          pppk_tmt_start: baseData.pppk_tmt_start,
        },
      },
      update: {
        pppk_employee_id: employee_id,
        ...baseData,
        pppk_create_by: SUPERADMIN_ID,
        pppk_create_date: now,
      },
      create: {
        pppk_employee_id: employee_id,
        ...baseData,
        pppk_create_by: SUPERADMIN_ID,
        pppk_create_date: now,
      },
    });

    return { employeeP3KRecord };
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

      await persistProfile(payload.data);
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
    logger.error(`[IMPORT P3K Records] Unexpected failure: ${error.message}`);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
