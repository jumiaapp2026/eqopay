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

process.on("unhandledRejection", (reason) => {
  logEvent("ERROR", "Unhandled Promise Rejection", {
    reason: reason instanceof Error ? reason.message : String(reason),
  });
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function normalize254Number(raw) {
  let cleaned = String(raw ?? "").trim().replace(/^\+/, "").replace(/\s+/g, "");
  if (/^0(7|1)\d{8}$/.test(cleaned)) {
    cleaned = `254${cleaned.slice(1)}`;
  }
  return /^254(7|1)\d{8}$/.test(cleaned) ? cleaned : null;
}

function sanitizeReferencePrefix(prefix) {
  const cleaned = String(prefix || "ORD")
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, 12);
  return cleaned || "ORD";
}

async function sendSingleStkPush(item) {
  const apiKey = process.env.EQOPAY_API_KEY?.trim();
  const email = process.env.EQOPAY_EMAIL?.trim();

  if (!apiKey || !email) {
    logEvent(
      "ERROR",
      "Missing environment variables: EQOPAY_API_KEY or EQOPAY_EMAIL is not set on Render.",
      { hasApiKey: Boolean(apiKey), hasEmail: Boolean(email) }
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

  logEvent("INFO", `Calling EQOPAY /v1/stkpush for ${item.msisdn}`, {
    amount: payload.amount,
    reference: payload.reference,
    email,
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

    const rawText = await response.text();
    let parsedBody;
    try {
      parsedBody = rawText ? JSON.parse(rawText) : {};
    } catch {
      parsedBody = { rawResponse: rawText };
    }

    if (response.status === 304) {
      logEvent("ERROR", `EQOPAY returned HTTP 304 for ${item.msisdn}`, {
        status: response.status,
        body: parsedBody,
      });
      return;
    }

    const accepted =
      response.ok &&
      (String(parsedBody?.ResponseCode) === "0" ||
        String(parsedBody?.success) === "200");

    if (!accepted) {
      knownErrorNumbers.add(item.msisdn);
      logEvent(
        "ERROR",
        `EQOPAY did not accept STK Push for ${item.msisdn} (HTTP ${response.status})`,
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
    logEvent("ERROR", `Fetch/Network error sending STK Push to ${item.msisdn}`, {
      errorName: err?.name,
      errorMessage: err?.message,
      cause: err?.cause ? String(err.cause) : undefined,
    });
  }
}

async function processQueue() {
  if (isProcessing) {
    logEvent("INFO", `Queue worker already running. Pending: ${queue.length}`);
    return;
  }
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
  } catch (err) {
    logEvent("ERROR", "Unexpected error inside processQueue loop", {
      errorMessage: err?.message || String(err),
    });
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
    envConfigured: Boolean(
      process.env.EQOPAY_API_KEY?.trim() && process.env.EQOPAY_EMAIL?.trim()
    ),
  });
});

// Direct single-number test endpoint for fast Render log verification
app.post("/api/test-stk", async (req, res) => {
  const normalized = normalize254Number(req.body?.msisdn);
  const amount = Math.max(1, Math.round(Number(req.body?.amount) || 1));

  if (!normalized) {
    logEvent("ERROR", "Test STK rejected: invalid phone number", {
      raw: req.body?.msisdn,
    });
    return res.status(400).json({ error: "Invalid phone number." });
  }

  knownErrorNumbers.delete(normalized);
  await sendSingleStkPush({
    msisdn: normalized,
    amount,
    reference: `TEST-${Date.now().toString().slice(-6)}`,
    accountNumber: req.body?.accountNumber ? String(req.body.accountNumber).trim() : "",
  });

  const latestLog = recentLogs[0] || null;
  return res.status(200).json({ ok: true, msisdn: normalized, latestLog });
});

// Enqueue phone numbers (ignores invalid & previously errored numbers)
app.post("/api/enqueue", (req, res) => {
  const { numbers, amount, referencePrefix, accountNumber } = req.body ?? {};
  logEvent("INFO", "Received /api/enqueue request", {
    rawCount: Array.isArray(numbers) ? numbers.length : 0,
    amount,
    referencePrefix,
  });

  if (!Array.isArray(numbers)) {
    logEvent("ERROR", "/api/enqueue rejected: numbers is not an array");
    return res.status(400).json({ error: "numbers must be an array." });
  }

  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount) || numericAmount < 1) {
    logEvent("ERROR", "/api/enqueue rejected: invalid amount", { amount });
    return res.status(400).json({ error: "Amount must be at least 1 KES." });
  }

  const refBase = sanitizeReferencePrefix(referencePrefix);
  const seenInBatch = new Set();
  const validItems = [];
  let ignoredInvalidCount = 0;
  let ignoredErrorCount = 0;
  let duplicatesSkipped = 0;

  numbers.forEach((raw, idx) => {
    const trimmed = String(raw ?? "").trim();
    if (!trimmed) return;

    const normalized = normalize254Number(trimmed);
    if (!normalized) {
      ignoredInvalidCount += 1;
      return;
    }

    if (knownErrorNumbers.has(normalized)) {
      ignoredErrorCount += 1;
      return;
    }

    if (seenInBatch.has(normalized)) {
      duplicatesSkipped += 1;
      return;
    }
    seenInBatch.add(normalized);

    validItems.push({
      msisdn: normalized,
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
  if (!process.env.EQOPAY_API_KEY?.trim() || !process.env.EQOPAY_EMAIL?.trim()) {
    logEvent(
      "ERROR",
      "Startup Warning: EQOPAY_API_KEY or EQOPAY_EMAIL environment variable is missing!"
    );
  }
});
