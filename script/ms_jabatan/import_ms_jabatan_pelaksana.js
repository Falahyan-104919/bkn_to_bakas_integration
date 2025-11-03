const fs = require("fs");
const path = require("path");

const { PrismaClient } = require("@prisma/client");
const logger = require("../logger");
const { parse } = require("csv-parse");

const prisma = new PrismaClient();
const PATH_CSV = path.resolve(__dirname, "ms_jabatan_pelaksana.csv");
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

const buildMSJabatan = async (value) => {
  const data = {
    jabatan_nama: value.Nama,
    jabatan_status: 1,
    jabatan_tipe: 2,
    jabatan_kode: value["ID"],
  };

  return Object.fromEntries(
    Object.entries(data).filter(([, value]) => value !== undefined),
  );
};

const persistMSJabatan = async (jabatan) => {
  const baseData = await buildMSJabatan(jabatan);
  const now = new Date();

  try {
    const { jabatan_id } = await prisma.ms_jabatan.upsert({
      where: {
        jabatan_kode: baseData.jabatan_kode,
      },
      create: {
        ...baseData,
        jabatan_create_by: SUPERADMIN_ID,
        jabatan_create_date: now,
      },
      update: {
        ...baseData,
        jabatan_create_by: SUPERADMIN_ID,
        jabatan_create_date: now,
      },
    });
    return { jabatan_id };
  } catch (error) {
    logger.error("[IMPORT] failed to import MS Jabatan Pelaksana", error);
    throw error;
  }
};

const importAllMSJabatan = async () => {
  try {
    const parser = fs
      .createReadStream(PATH_CSV)
      .pipe(
        parse({
          columns: true,
          skip_empty_lines: true,
          delimiter: ";",
        }),
      );

    let processedCount = 0;

    for await (const record of parser) {
      try {
        await persistMSJabatan(record);
        processedCount += 1;
        logger.info(`[IMPORT] Imported ${record.ID}`);
      } catch (error) {
        logger.error(
          `[IMPORT] Skipped record ${record.ID}: ${error.message}`,
        );
      }
    }

    logger.info(
      `[IMPORT] Finished importing MS Jabatan Pelaksana. Total processed: ${processedCount}`,
    );
  } catch (error) {
    logger.error(`[IMPORT] Failed to to read csv : ${error.message}`);
  }
};

importAllMSJabatan()
  .catch((error) => {
    logger.error(
      `[IMPORT MS Jabatan Pelaksana] Unexpected failure: ${error.message}`,
    );
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
