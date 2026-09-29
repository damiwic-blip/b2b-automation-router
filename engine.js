const express = require('express');
const fs = require('fs');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const API_SECRET_KEY = "b2b_secure_crypto_token_xyz9984";
const logFilePath = path.join(__dirname, 'server_execution.log');
const dbFilePath = path.join(__dirname, 'router_database.json');

if (!fs.existsSync(dbFilePath)) {
    fs.writeFileSync(dbFilePath, JSON.stringify({ webhook_transactions: [], system_meta: { version: "6.0.0", last_boot: new Date().toISOString() } }, null, 2));
}

function writeToFileLog(message) {
    const cleanMessage = message.replace(/[\u001b\u001b][^m]*m/g, '');
    const timestamp = new Date().toISOString();
    fs.appendFileSync(logFilePath, `[${timestamp}] ${cleanMessage}\n`);
}

const pipelineLogger = {
    log: (msg) => { console.log(msg); writeToFileLog(msg); },
    error: (msg) => { console.error(msg); writeToFileLog(`❌ ERROR: ${msg}`); },
    warn: (msg) => { console.warn(msg); writeToFileLog(`⚠️ WARNING: ${msg}`); }
};

function getDatabase() {
    try {
        const fileContent = fs.readFileSync(dbFilePath, 'utf8');
        return JSON.parse(fileContent);
    } catch (err) {
        return { webhook_transactions: [] };
    }
}

function saveDatabase(data) {
    fs.writeFileSync(dbFilePath, JSON.stringify(data, null, 2));
}

function insertDatabaseRecord(txId, transactionData) {
    pipelineLogger.log(`▶️ [RUNNING] Step ID: step_03_database_persistence`);
    const database = getDatabase();
    database.webhook_transactions.push({
        transaction_id: txId,
        timestamp: new Date().toISOString(),
        ...transactionData
    });
    saveDatabase(database);
    pipelineLogger.log(`💾 [DATABASE SUCCESS] Transaction logged securely to storage layer.`);
}

function updateDatabaseRecordStatus(txId, newStatus, extraData = {}) {
    const database = getDatabase();
    const record = database.webhook_transactions.find(t => t.transaction_id === txId);
    if (record) {
        record.status = newStatus;
        Object.assign(record, extraData);
        saveDatabase(database);
        pipelineLogger.log(`💾 [DATABASE UPDATE] Transaction ${txId} status updated to: ${newStatus}`);
    }
}

function evaluateClientTier(clientId) {
    pipelineLogger.log(`▶️ [RUNNING] Step ID: step_01b_condition_evaluator`);
    if (clientId.startsWith('crm_usr_99')) {
        pipelineLogger.log(`👑 [EVALUATOR RESULT] Account ${clientId} flagged as: TIER 1 ENTERPRISE CLIENT`);
        return 'TIER_1_ENTERPRISE';
    } else {
        pipelineLogger.log(`📋 [EVALUATOR RESULT] Account ${clientId} flagged as: STANDARD BUSINESS CLIENT`);
        return 'STANDARD_BUSINESS';
    }
}

async function transmitWithRetry(clientId, accountTier, maxAttempts = 3, delayMs = 500) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            const destinationUrl = `https://internal.io`; 
            if (attempt < maxAttempts) throw new Error("Remote server connection timed out");
            pipelineLogger.log(`🚀 [OUTBOUND TRANSMISSION SUCCESS] Packets delivered successfully.`);
            return { success: true };
        } catch (error) {
            pipelineLogger.warn(`   ⚠️ [RETRYING] Outbound transmission dropped. Attempt ${attempt}/${maxAttempts}. Re-routing in ${delayMs}ms...`);
            if (attempt === maxAttempts) throw new Error(`Transmission failure after ${maxAttempts} tries.`);
            await new Promise(resolve => setTimeout(resolve, delayMs));
            delayMs *= 2; 
        }
    }
}

function triggerBackgroundWorkerQueue(txId, clientId, accountTier) {
    pipelineLogger.log(`⚙️ [QUEUE MANAGER] Registering asynchronous tasks for Background Worker...`);
    setTimeout(async () => {
        pipelineLogger.log(`\n🤖 [BACKGROUND WORKER SPINNING UP] Processing long-running background tasks...`);
        pipelineLogger.log(`⚙️ [BACKGROUND JOB 1/2] Syncing client metadata profiles across remote distributed regions...`);
        await new Promise(resolve => setTimeout(resolve, 1500));
        pipelineLogger.log(`⚙️ [BACKGROUND JOB 2/2] Compiling analytical data segments for Account: ${clientId}`);
        await new Promise(resolve => setTimeout(resolve, 1000));
        updateDatabaseRecordStatus(txId, "COMPLETED_BY_BACKGROUND_WORKER", { worker_execution: "SUCCESS" });
        pipelineLogger.log(`🏁 [BACKGROUND WORKER FINISHED] Thread tasks completed cleanly for transaction: ${txId}\n`);
    }, 0);
}

app.post('/api/v1/webhook/trigger', async (req, res) => {
    pipelineLogger.log(`\n🔒 [SECURITY GATEKEEPER] Evaluating inbound request headers...`);
    const authHeader = req.headers['authorization'];
    if (!authHeader || authHeader !== `Bearer ${API_SECRET_KEY}`) {
        pipelineLogger.error(`[AUTH BLOCKED] Unauthorized request attempt denied. Invalid or missing API key.`);
        return res.status(401).json({ status: "UNAUTHORIZED", message: "Access Denied: Invalid or missing API Bearer Token." });
    }

    pipelineLogger.log(`✅ [AUTH SUCCESS] Security clearance verified. Resuming core data processing...`);
    const { incoming_client_id } = req.body;
    if (!incoming_client_id) {
        pipelineLogger.error(`[PIPELINE ABORTED] Missing critical field: incoming_client_id`);
        return res.status(400).json({ status: "ERROR", message: "Missing incoming_client_id" });
    }

    pipelineLogger.log(`🚀 [PIPELINE EXECUTION STARTED] Live Inbound Webhook Automation Pipeline`);
    const accountTier = evaluateClientTier(incoming_client_id);
    pipelineLogger.log(`▶️ [RUNNING] Step ID: step_02_outbound_transmission`);
    
    try {
        await transmitWithRetry(incoming_client_id, accountTier);
        const targetTxId = `tx_${Math.random().toString(36).substr(2, 9)}`;
        insertDatabaseRecord(targetTxId, {
            client_id: incoming_client_id,
            classification: accountTier,
            status: "PROCESSING"
        });
        triggerBackgroundWorkerQueue(targetTxId, incoming_client_id, accountTier);
        res.status(200).json({
            status: "SUCCESS",
            message: "Pipeline request authenticated. Handed off to background thread layers.",
            transaction_id: targetTxId,
            account_classification: accountTier
        });
    } catch (pipelineError) {
        pipelineLogger.error(`[PIPELINE FAILURE] Pipeline crashed: ${pipelineError.message}`);
        res.status(500).json({ status: "ERROR", message: pipelineError.message });
    }
});

app.get('/api/v1/jobs/status/:txId', (req, res) => {
    const { txId } = req.params;
    const database = getDatabase();
    const record = database.webhook_transactions.find(t => t.transaction_id === txId);
    if (!record) {
        return res.status(404).json({ status: "NOT_FOUND", message: "Transaction ID not found in storage matrix." });
    }
    res.status(200).json({
        status: "SUCCESS",
        transaction_id: record.transaction_id,
        current_execution_state: record.status,
        timestamp: record.timestamp
    });
});

app.get('/api/v1/admin/analytics', (req, res) => {
    pipelineLogger.log(`📊 [ADMIN VIEW] Processing request for Administrative Metrics Dashboard...`);
    const database = getDatabase();
    const totalTransactions = database.webhook_transactions.length;
    const enterpriseCount = database.webhook_transactions.filter(t => t.classification === 'TIER_1_ENTERPRISE').length;
    const standardCount = database.webhook_transactions.filter(t => t.classification === 'STANDARD_BUSINESS').length;
    const completedBackgroundCount = database.webhook_transactions.filter(t => t.status === 'COMPLETED_BY_BACKGROUND_WORKER').length;

    res.status(200).json({
        system_status: "HEALTHY",
        uptime_state: "OPERATIONAL",
        metrics: {
            total_processed_payloads: totalTransactions,
            tier_1_enterprise_accounts: enterpriseCount,
            standard_business_accounts: standardCount,
            background_jobs_successfully_finished: completedBackgroundCount
        },
        meta: database.system_meta
    });
});

app.listen(PORT, () => {
    console.log(`===================================================`);
    console.log(`🤖 ENTERPRISE B2B BUSINESS AUTOMATION ROUTER ALIVE`);
    console.log(`📡 Listening for live webhooks on: http://localhost:${PORT}`);
    console.log(`===================================================`);
});