const fsp = require("fs").promises;
const path = require("path");
const bcrypt = require("bcrypt");

const { PrismaClient } = require("@prisma/client");
const logger = require("../logger");

const prisma = new PrismaClient();

const STAGING_DATA_DIR = path.resolve(__dirname, "staging_employee");
const SUPERADMIN_ID = 1;
const DEFAULT_ROLE_ID = 3;

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

const mapGender = (value) => {
  const cleaned = toNullIfEmpty(value);
  if (!cleaned) return null;
  const normalized = cleaned.toUpperCase();
  if (normalized === "M") return 1;
  if (normalized === "F") return 2;
  return null;
};

const buildEmployeeData = async (profile) => {
  const primaryPhone =
    toNullIfEmpty(profile.noHp) || toNullIfEmpty(profile.noTelp);
  const primaryEmail =
    toNullIfEmpty(profile.email) || toNullIfEmpty(profile.emailGov);
  const domicilePostal = toInt(profile.kodePos);
  const { religion_id } = await prisma.ms_religion.findFirst({
    where: {
      religion_kode: parseInt(toNullIfEmpty(profile.agamaId)),
    },
  });

  const data = {
    employee_fullname: toNullIfEmpty(profile.nama),
    employee_nip: toNullIfEmpty(profile.nipBaru),
    employee_address: toNullIfEmpty(profile.alamat),
    employee_oldnip: toNullIfEmpty(profile.nipLama),
    employee_phone: primaryPhone,
    employee_email: primaryEmail,
    employee_status: toInt(profile.statusHidup),
    employee_gender: mapGender(profile.jenisKelamin),
    employee_religion: religion_id,
    employee_dateofbirth: parseDate(profile.tglLahir),
    employee_placeofbirth: toNullIfEmpty(profile.tempatLahir),
    employee_npwp: toNullIfEmpty(profile.noNpwp),
    employee_bpjs: toNullIfEmpty(profile.bpjs),
    employee_nik: toNullIfEmpty(profile.nik),
    employee_gelardepan: toNullIfEmpty(profile.gelarDepan),
    employee_gelarbelakang: toNullIfEmpty(profile.gelarBelakang),
    employee_karpeg: toNullIfEmpty(profile.noSeriKarpeg),
    employee_alamat_domisili: toNullIfEmpty(profile.alamat),
    employee_pos_domisili: domicilePostal,
    employee_taspen: toNullIfEmpty(profile.noTaspen),
    employee_karis_karsu: toNullIfEmpty(profile.karis_karsu),
    employee_pmk_tahun: toInt(profile.mkTahun),
    employee_pmk_bulan: toInt(profile.mkBulan),
    employee_pmk_tmt: parseDate(profile.tmtCpns) || parseDate(profile.tmtPns),
    employee_status_asn: 2,
    employee_bkn: toNullIfEmpty(profile.id),
  };

  return Object.fromEntries(
    Object.entries(data).filter(([, value]) => value !== undefined),
  );
};

const persistProfile = async (profile) => {
  const baseData = await buildEmployeeData(profile);
  if (!baseData.employee_nip) {
    throw new Error("Missing NIP (employee_nip) in profile payload");
  }

  const now = new Date();

  return prisma.$transaction(async (tx) => {
    const employeeRecord = await tx.ms_employee.upsert({
      where: {
        employee_nip_employee_nik: {
          employee_nip: baseData.employee_nip,
          employee_nik: baseData.employee_nik,
        },
      },
      update: {
        ...baseData,
        employee_update_by: SUPERADMIN_ID,
        employee_update_date: now,
      },
      create: {
        ...baseData,
        employee_create_by: SUPERADMIN_ID,
        employee_create_date: now,
      },
    });

    const hashedPassword = bcrypt.hashSync(employeeRecord.employee_nip, 10);

    let sysUserRecord = await tx.sys_user.upsert({
      where: { user_nip: baseData.employee_nip },
      update: {
        user_nip: baseData.employee_nip,
        user_name: baseData.employee_nip,
        user_password: hashedPassword,
        user_access_id: 3,
        user_create_by: 1,
        user_create_date: now,
        user_status: 1,
        user_default_role_id: 3,
      },
      create: {
        user_nip: baseData.employee_nip,
        user_name: baseData.employee_nip,
        user_password: hashedPassword,
        user_access_id: 3,
        user_create_by: 1,
        user_create_date: now,
        user_status: 1,
        user_default_role_id: 3,
      },
    });

    await tx.sys_userrole.create({
      data: {
        userId: sysUserRecord.user_id,
        roleId: DEFAULT_ROLE_ID,
      },
    });

    return { employeeRecord, sysUserRecord };
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
    logger.error(`[IMPORT] Unexpected failure: ${error.message}`);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
