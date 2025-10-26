const winston = require("winston");
const { combine, timestamp, printf, colorize, align } = winston.format;

// This defines the text format for our logs
const logFormat = printf(({ level, message, timestamp }) => {
  return `${timestamp} ${level}: ${message}`;
});

const logger = winston.createLogger({
  level: "info", // This means it will log 'info', 'warn', and 'error' messages
  format: combine(
    timestamp({ format: "YYYY-MM-DD HH:mm:ss" }), // Add a timestamp
    logFormat,
  ),
  // Define "transports" (where the logs should go)
  transports: [
    // 1. Write all 'error' level logs to errors.log
    new winston.transports.File({ filename: "errors.log", level: "error" }),
    // 2. Write all logs (info and above) to all.log
    new winston.transports.File({ filename: "all.log" }),
  ],
  // If an error happens in the script, don't crash the logger
  exitOnError: false,
});

// 3. Also log everything to the console (so you can still see it)
logger.add(
  new winston.transports.Console({
    format: combine(colorize(), align(), logFormat),
  }),
);

module.exports = logger;
