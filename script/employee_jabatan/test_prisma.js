const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient({});

const getEmployee = async (nip) => {
  try {
    const employee = await prisma.ms_employee.findFirstOrThrow({
      where: { employee_nip: nip, employee_status: { notIn: [0] } },
    });
    return console.log("EMPLOYEE ", employee);
  } catch (error) {
    console.error("Error fetching employee:", error);
    throw error;
  }
};

const getJabatan = async (kode_bkn) => {
  try {
    const jabatan = await prisma.ms_jabatan.findFirstOrThrow({
      where: { jabatan_kode: kode_bkn, jabatan_status: { notIn: [0] } },
    });
    return console.log("JABATAN ", jabatan);
  } catch (err) {
    console.error("Error fetching ms_jabatan:", err);
    throw err;
  }
};

const getOrganization = async (kode_bkn) => {
  try {
    const organization = await prisma.ms_organization.findFirstOrThrow({
      where: {
        organization_bkn_id: kode_bkn,
        organization_status: { notIn: [0] },
      },
    });
    return console.log("ORGANIZATION ", organization);
  } catch (err) {
    console.error("Error fetching ms_organization :", err);
    throw err;
  }
};

getEmployee("197007241996031003");
getJabatan("8ae483c57f6de4e3017f7336a9a52fd1");
getOrganization("ff80808134b2e6b70134bc4ca9051e3e");
