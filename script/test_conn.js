require("dotenv").config();
const axios = require("axios");
const { URLSearchParams } = require("url");
const fs = require("fs");
const path = require("path");

// --- Configuration ---
const API_BASE_URL = process.env.API_BASE_URL;
const TOKEN_URL = process.env.TOKEN_URL;
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const STATIC_AUTH_TOKEN = process.env.STATIC_AUTH_TOKEN;
const TEST_NIP = "197007241996031003";

// NEW: Define the download endpoint path
const DOWNLOAD_PATH = "/download-dok";
const DOWNLOAD_DIR = path.join(__dirname, "temp_downloads");
// --- End Configuration ---

/**
 * Re-creation of your server's getTokenAuthor() function
 */
async function fetchDynamicToken() {
  console.log(`[AUTH] Requesting dynamic token from: ${TOKEN_URL}`);
  const body = new URLSearchParams();
  body.append("grant_type", "client_credentials");
  body.append("client_id", CLIENT_ID);
  body.append("client_secret", CLIENT_SECRET);
  try {
    const response = await axios.post(TOKEN_URL, body, {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
    });
    console.log("[AUTH] Successfully fetched dynamic token.");
    return response.data;
  } catch (error) {
    console.error("[AUTH] ❌ FAILED to fetch dynamic token:");
    if (error.response) {
      console.error(`Status: ${error.response.status}`);
      console.error("Data:", error.response.data);
    } else {
      console.error("Error:", error.message);
    }
    throw new Error("Could not fetch dynamic token.");
  }
}

/**
 * Main test function
 */
async function testConnection() {
  console.log("--- Starting API Connection Test ---");

  // --- 1. Check if all .env variables loaded ---
  if (
    !API_BASE_URL ||
    !TOKEN_URL ||
    !CLIENT_ID ||
    !CLIENT_SECRET ||
    !STATIC_AUTH_TOKEN
  ) {
    console.error("--- ❌ FAILED! ---");
    console.error(
      "Error: One or more required variables are missing from .env.",
    );
    console.log("--- Test Aborted ---");
    return;
  }

  // Create download directory if it doesn't exist
  if (!fs.existsSync(DOWNLOAD_DIR)) {
    fs.mkdirSync(DOWNLOAD_DIR);
    console.log(`[SETUP] Created download directory: ${DOWNLOAD_DIR}`);
  }

  try {
    // --- Step 1: Fetch the dynamic token ---
    const dynamicTokenData = await fetchDynamicToken();
    const dynamicToken = dynamicTokenData.access_token;
    if (!dynamicToken) {
      console.error("--- ❌ FAILED! ---");
      console.error("Error: Token endpoint did not return an access_token.");
      return;
    }

    // --- Step 2: Call the 'jabatan' API ---
    console.log("[API] Using tokens to call the main API...");
    const apiUrl = `${API_BASE_URL}/jabatan/pns/${TEST_NIP}`;

    // Create reusable auth headers object
    const authHeaders = {
      accept: "application/json", // Accept JSON for this request
      Authorization: `Bearer ${dynamicToken}`,
      Auth: `Bearer ${STATIC_AUTH_TOKEN}`,
    };

    console.log(`[API] Attempting to call: ${apiUrl}`);
    const response = await axios.get(apiUrl, { headers: authHeaders });

    console.log("\n--- ✅ SUCCESS (Step 2)! ---");
    console.log("Fetched JSON data for NIP.");

    // --- Step 3: Parse JSON and Download Files ---
    console.log("\n--- Starting Step 3: File Download Simulation ---");
    const historyRecords = response.data.data;

    for (const record of historyRecords) {
      if (!record.path || Object.keys(record.path).length === 0) {
        console.log(`[INFO] No files found for record ${record.id}.`);
        continue;
      }

      console.log(`[INFO] Found files for record ${record.id}.`);

      for (const docKey in record.path) {
        const fileInfo = record.path[docKey];

        // --- THIS IS THE FIX ---
        // 1. Get the un-encoded file path from the JSON
        const filePath = fileInfo.dok_uri;

        // 2. URL-encode it (converts '/' to '%2F', etc.)
        const encodedFilePath = encodeURIComponent(filePath);

        // 3. Build the exact URL from your curl command
        const downloadUrl = `${API_BASE_URL}${DOWNLOAD_PATH}?filePath=${encodedFilePath}`;
        // --- END FIX ---

        const safeFilename = `${record.id}_${fileInfo.dok_id}_${path.basename(filePath)}`;
        const localFilePath = path.join(DOWNLOAD_DIR, safeFilename);

        try {
          console.log(
            `[DOWNLOAD] Attempting to download: ${fileInfo.dok_nama} from ${downloadUrl}`,
          );

          const fileResponse = await axios.get(downloadUrl, {
            // REUSE the same auth headers
            headers: {
              ...authHeaders,
              accept: "application/pdf", // Tell the server we want a file
            },
            responseType: "stream", // Handle as a file stream
          });

          const writer = fs.createWriteStream(localFilePath);
          fileResponse.data.pipe(writer);

          await new Promise((resolve, reject) => {
            writer.on("finish", resolve);
            writer.on("error", reject);
          });

          console.log(`[SUCCESS] Saved file to: ${localFilePath}`);
        } catch (fileError) {
          console.error(`[FAIL] Failed to download ${fileInfo.dok_nama}`);
          if (fileError.response) {
            console.error(`Status: ${fileError.response.status}`);
          } else {
            console.error(fileError.message);
          }
        }
      }
    }
  } catch (error) {
    console.error("\n--- ❌ FAILED! ---");
    if (error.response) {
      console.error(`Status: ${error.response.status}`);
      console.error(
        "Response Data:",
        JSON.stringify(error.response.data, null, 2),
      );
    } else {
      console.error("Script Error:", error.message);
    }
  }

  console.log("\n--- Test Finished ---");
}

// Run the test
testConnection();
