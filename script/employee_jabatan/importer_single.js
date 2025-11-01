const { processNip, prisma } = require("./importer");
const logger = require("./logger");

/* new NIP to be fetched
197809102014072005,
199501012017081003,
200008022024091001,
200206122024091002,
198003062024212003,
199809072022081001,
200011292022081002,
200007052023081001,
200006112023082001,
198209092014032002,
199007292017081001,
199712112024212013,
*/

async function run() {
  const targetNip = process.argv[2];

  if (!targetNip) {
    console.error("Usage: node script/importer_single.js <NIP>");
    process.exit(1);
  }

  logger.info(`--- Starting Single-NIP Importer for ${targetNip} ---`);
  const processed = await processNip(targetNip);

  if (processed) {
    logger.info(`[DONE] Completed processing for NIP ${targetNip}`);
  } else {
    logger.warn(
      `[DONE] Nothing processed for NIP ${targetNip} (see logs above).`,
    );
    process.exitCode = 1;
  }
}

run()
  .catch((err) => {
    logger.error(
      `[FATAL] The single importer encountered an error: ${err.message}`,
    );
    logger.error(err.stack);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
    logger.info("--- Database disconnected ---");
  });
