const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

const EQOPAY_URL = 'https://api.eqopay.co.ke/v1/stkpush';
const EQOPAY_API_KEY = process.env.EQOPAY_API_KEY || '';
const EQOPAY_EMAIL = process.env.EQOPAY_EMAIL || '';

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// In-memory job state
let jobState = {
  running: false,
  total: 0,
  processed: 0,
  logs: []
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Normalize phone numbers to 07XXXXXXXX or 01XXXXXXXX format
function formatMsisdn(raw) {
  const digits = raw.replace(/\D/g, '');
  if (digits.startsWith('254') && digits.length === 12) {
    return '0' + digits.slice(3);
  }
  if ((digits.startsWith('7') || digits.startsWith('1')) && digits.length === 9) {
    return '0' + digits;
  }
  return digits;
}

// Health check endpoint (useful for Render & uptime monitors)
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok', running: jobState.running });
});

// Get live batch status for the UI
app.get('/api/status', (req, res) => {
  res.json(jobState);
});

// Start sequential STK push batch
app.post('/api/start', async (req, res) => {
  if (jobState.running) {
    return res.status(409).json({ error: 'A batch is already running. Wait for it to finish.' });
  }

  if (!EQOPAY_API_KEY || !EQOPAY_EMAIL) {
    const msg = 'Missing EQOPAY_API_KEY or EQOPAY_EMAIL in server environment variables.';
    console.error(`[CONFIG ERROR] ${msg}`);
    return res.status(500).json({ error: msg });
  }

  const { numbers, amount, referencePrefix } = req.body;

  if (!Array.isArray(numbers) || numbers.length === 0) {
    return res.status(400).json({ error: 'Provide at least one phone number.' });
  }

  const parsedAmount = String(amount || '100').trim();
  const prefix = String(referencePrefix || 'ORDER').trim();

  const cleanedNumbers = numbers
    .map((n) => formatMsisdn(String(n)))
    .filter((n) => n.length >= 10);

  if (cleanedNumbers.length === 0) {
    return res.status(400).json({ error: 'No valid phone numbers found after formatting.' });
  }

  jobState = {
    running: true,
    total: cleanedNumbers.length,
    processed: 0,
    logs: []
  };

  res.json({ message: 'Batch started', total: cleanedNumbers.length });

  // Run sequential loop in background
  (async () => {
    console.log(`\n=== [BATCH STARTED] Total numbers: ${cleanedNumbers.length} | Amount: KES ${parsedAmount} ===`);

    for (let i = 0; i < cleanedNumbers.length; i++) {
      const msisdn = cleanedNumbers[i];
      const reference = `${prefix}-${Date.now()}-${i + 1}`;
      const payload = {
        api_key: EQOPAY_API_KEY,
        email: EQOPAY_EMAIL,
        amount: parsedAmount,
        msisdn,
        reference
      };

      console.log(`[${i + 1}/${cleanedNumbers.length}] Sending STK Push to ${msisdn} (Ref: ${reference})...`);

      try {
        const response = await fetch(EQOPAY_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        const rawText = await response.text();
        let data;
        try {
          data = JSON.parse(rawText);
        } catch {
          data = { rawResponse: rawText };
        }

        if (!response.ok || (data.ResponseCode && String(data.ResponseCode) !== '0')) {
          const errDetail = `HTTP ${response.status} | Response: ${JSON.stringify(data)}`;
          console.error(`[STK ERROR] Number: ${msisdn} | Ref: ${reference} | ${errDetail}`);
          jobState.logs.push({
            index: i + 1,
            msisdn,
            reference,
            status: 'FAILED',
            detail: errDetail,
            time: new Date().toISOString()
          });
        } else {
          const okDetail = data.message || 'STK Push dispatched';
          console.log(`[STK SUCCESS] Number: ${msisdn} | RequestID: ${data.transaction_request_id || 'N/A'} | ${okDetail}`);
          jobState.logs.push({
            index: i + 1,
            msisdn,
            reference,
            status: 'SENT',
            detail: okDetail,
            time: new Date().toISOString()
          });
        }
      } catch (err) {
        const netErr = err.stack || err.message || String(err);
        console.error(`[NETWORK/FETCH ERROR] Number: ${msisdn} | Ref: ${reference}\n${netErr}`);
        jobState.logs.push({
          index: i + 1,
          msisdn,
          reference,
          status: 'ERROR',
          detail: err.message || 'Network failure',
          time: new Date().toISOString()
        });
      }

      jobState.processed = i + 1;

      // Wait 5 seconds before sending the next request (skip delay after the last number)
      if (i < cleanedNumbers.length - 1) {
        console.log(`Waiting 5 seconds before next request...`);
        await sleep(5000);
      }
    }

    jobState.running = false;
    console.log(`=== [BATCH COMPLETED] Processed ${jobState.processed}/${jobState.total} ===\n`);
  })();
});

// Optional webhook receiver to log callback results in Render
app.post('/webhook/eqopay', (req, res) => {
  console.log('[EQOPAY WEBHOOK RECEIVED]', JSON.stringify(req.body));
  res.status(200).json({ received: true });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
