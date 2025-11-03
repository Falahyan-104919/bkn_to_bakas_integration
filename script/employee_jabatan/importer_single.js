const { processNip, prisma } = require("./importer");
const logger = require("./logger");

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
