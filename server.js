import express from "express";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const INTERVAL_MS = 5000; // 5-second delay between requests
const REQUEST_TIMEOUT_MS = 30000;

app.disable("etag");
app.use(express.json({ limit: "256kb" }));
app.use(express.static(path.join(__dirname, "public")));

app.use("/api", (req, res, next) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
  next();
});

// In-memory queue, error tracking & run state
let queue = [];
let isProcessing = false;
let shouldStop = false;
let recentLogs = [];
const knownErrorNumbers = new Set();

function logEvent(level, message, details = null) {
  const timestamp = new Date().toISOString();
  const formattedDetails =
    details !== null
      ? typeof details === "string"
        ? details
        : JSON.stringify(details)
      : "";

  const line = `[${timestamp}] [${level}] ${message}${
    formattedDetails ? ` | ${formattedDetails}` : ""
  }`;

  if (level === "ERROR") {
    console.error(line);
  } else {
    console.log(line);
  }

  recentLogs.unshift({ timestamp, level, message, details });
  if (recentLogs.length > 100) recentLogs.pop();
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isValid254Number(msisdn) {
  return /^254(7|1)\d{8}$/.test(msisdn);
}

function sanitizeReferencePrefix(prefix) {
  const cleaned = String(prefix || "ORD")
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, 12);
  return cleaned || "ORD";
}

async function sendSingleStkPush(item) {
  const apiKey = process.env.EQOPAY_API_KEY;
  const email = process.env.EQOPAY_EMAIL;

  if (!apiKey || !email) {
    logEvent(
      "ERROR",
      "Missing environment variables: EQOPAY_API_KEY or EQOPAY_EMAIL is not set on Render."
    );
    return;
  }

  if (knownErrorNumbers.has(item.msisdn)) {
    logEvent("INFO", `Skipping known error number ${item.msisdn}`);
    return;
  }

  const payload = {
    api_key: apiKey,
    email: email,
    amount: String(item.amount),
    msisdn: item.msisdn,
    reference: item.reference,
  };

  if (item.accountNumber) {
    payload.account_number = item.accountNumber;
  }

  logEvent("INFO", `Sending STK Push to ${item.msisdn}`, {
    amount: payload.amount,
    reference: payload.reference,
  });

  try {
    const response = await fetch("https://api.eqopay.co.ke/v1/stkpush", {
      method: "POST",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        "Cache-Control": "no-cache",
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });

    if (response.status === 304) {
      logEvent("WARN", `Received HTTP 304 Not Modified for ${item.msisdn} (empty body)`);
      return;
    }

    const rawText = await response.text();
    let parsedBody;
    try {
      parsedBody = rawText ? JSON.parse(rawText) : {};
    } catch {
      parsedBody = { rawResponse: rawText };
    }

    if (!response.ok || String(parsedBody?.ResponseCode) !== "0") {
      knownErrorNumbers.add(item.msisdn);
      logEvent(
        "ERROR",
        `EQOPAY API rejected request for ${item.msisdn} (HTTP ${response.status}) - marked as error number`,
        parsedBody
      );
    } else {
      logEvent(
        "SUCCESS",
        `STK Push accepted for ${item.msisdn}`,
        parsedBody
      );
    }
  } catch (err) {
    knownErrorNumbers.add(item.msisdn);
    logEvent("ERROR", `Network/Fetch failure for ${item.msisdn} - marked as error number`, {
      errorName: err.name,
      errorMessage: err.message,
      cause: err.cause ? String(err.cause) : undefined,
    });
  }
}

async function processQueue() {
  if (isProcessing) return;
  isProcessing = true;
  shouldStop = false;

  logEvent("INFO", `Queue worker started. Items pending: ${queue.length}`);

  try {
    while (queue.length > 0 && !shouldStop) {
      const nextItem = queue.shift();
      await sendSingleStkPush(nextItem);

      if (queue.length > 0 && !shouldStop) {
        logEvent("INFO", `Waiting ${INTERVAL_MS / 1000}s before next request...`);
        await sleep(INTERVAL_MS);
      }
    }
  } finally {
    isProcessing = false;
    logEvent(
      "INFO",
      shouldStop
        ? `Queue worker stopped manually. Remaining items: ${queue.length}`
        : "Queue worker finished all items."
    );
  }
}

// Health endpoint for Render & uptime monitors
app.get("/health", (req, res) => {
  res.set("Cache-Control", "no-store");
  res.status(200).json({
    status: "ok",
    isProcessing,
    queuedCount: queue.length,
    knownErrorCount: knownErrorNumbers.size,
    envConfigured: Boolean(process.env.EQOPAY_API_KEY && process.env.EQOPAY_EMAIL),
  });
});

// Enqueue phone numbers (ignores invalid & previously errored numbers)
app.post("/api/enqueue", (req, res) => {
  const { numbers, amount, referencePrefix, accountNumber } = req.body ?? {};

  if (!Array.isArray(numbers)) {
    return res.status(400).json({ error: "numbers must be an array." });
  }

  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount) || numericAmount < 1) {
    return res.status(400).json({ error: "Amount must be at least 1 KES." });
  }

  const refBase = sanitizeReferencePrefix(referencePrefix);
  const seenInBatch = new Set();
  const validItems = [];
  let ignoredInvalidCount = 0;
  let ignoredErrorCount = 0;
  let duplicatesSkipped = 0;

  numbers.forEach((raw, idx) => {
    const cleaned = String(raw ?? "").trim().replace(/^\+/, "");
    if (!cleaned) return;

    if (!isValid254Number(cleaned)) {
      ignoredInvalidCount += 1;
      return;
    }

    if (knownErrorNumbers.has(cleaned)) {
      ignoredErrorCount += 1;
      return;
    }

    if (seenInBatch.has(cleaned)) {
      duplicatesSkipped += 1;
      return;
    }
    seenInBatch.add(cleaned);

    validItems.push({
      msisdn: cleaned,
      amount: Math.round(numericAmount),
      reference: `${refBase}-${Date.now().toString().slice(-5)}-${idx + 1}`,
      accountNumber: accountNumber ? String(accountNumber).trim() : "",
    });
  });

  if (validItems.length > 0) {
    queue.push(...validItems);
    processQueue();
  }

  logEvent("INFO", `Enqueue processed: ${validItems.length} added`, {
    ignoredInvalidCount,
    ignoredErrorCount,
    duplicatesSkipped,
    totalInQueue: queue.length,
  });

  return res.status(200).json({
    queued: validItems.length,
    ignoredInvalidCount,
    ignoredErrorCount,
    duplicatesSkipped,
    totalInQueue: queue.length,
  });
});

// Stop & clear queue
app.post("/api/stop", (req, res) => {
  shouldStop = true;
  const cleared = queue.length;
  queue = [];
  logEvent("INFO", `Stop requested. Cleared ${cleared} pending item(s).`);
  res.status(200).json({ stopped: true, cleared });
});

// Status & live logs for the UI
app.get("/api/status", (req, res) => {
  res.status(200).json({
    isProcessing,
    queuedCount: queue.length,
    knownErrorNumbers: Array.from(knownErrorNumbers),
    nextNumbers: queue.slice(0, 10).map((i) => i.msisdn),
    logs: recentLogs,
  });
});

app.listen(PORT, () => {
  logEvent("INFO", `EQOPAY STK Bot listening on port ${PORT}`);
  if (!process.env.EQOPAY_API_KEY || !process.env.EQOPAY_EMAIL) {
    logEvent(
      "ERROR",
      "Startup Warning: EQOPAY_API_KEY or EQOPAY_EMAIL environment variable is missing!"
    );
  }
});
