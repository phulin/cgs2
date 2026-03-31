import { Hono } from "hono";
import {
	extractBearerToken,
	signCallbackToken,
	verifyCallbackToken,
} from "./lib/callback-auth";
import { IngestContainer } from "./lib/ingest-container";
import {
	completePlanning,
	createIngestJob,
	createJobUnits,
	incrementProcessedTitles,
	incrementUnitProcessedNodes,
	markUnitCompleted,
	markUnitRunning,
	recordTitleError,
} from "./lib/ingest-jobs";
import { hash64, hash64ToHex } from "./lib/packfile/hash";
import { PackfileDO } from "./lib/packfile-do";
import { getSourceConfig, validateSourceCode } from "./lib/sources-config";
import { SearchExportWorkflow } from "./lib/search/workflow";
import { VectorIngestWorkflow } from "./lib/vector/workflow";
import {
	computeDiff,
	ensureSourceVersion,
	getOrCreateSource,
	insertNodes,
} from "./lib/versioning";
import type {
	Env,
	IngestNode,
	NodeMeta,
	NodePayload,
	SearchExportWorkflowParams,
	VectorWorkflowParams,
} from "./types";

type AppContext = {
	Bindings: Env;
};

const app = new Hono<AppContext>();

// ──────────────────────────────────────────────────────────────
// Classification Tables Ingest
// ──────────────────────────────────────────────────────────────
import * as cheerio from "cheerio";

app.post("/api/admin/ingest-classifications", async (c) => {
	const urls = [
		"https://uscode.house.gov/classification/tables.shtml",
		"https://uscode.house.gov/classification/priortables.shtml",
	];

	const targetHrefs: string[] = [];

	for (const url of urls) {
		const res = await fetch(url);
		if (!res.ok) continue;
		const html = await res.text();
		const $ = cheerio.load(html);

		$("a").each((_, el) => {
			const href = $(el).attr("href");
			if (href?.match(/tbl\d+pl(_\w+)?\.htm/)) {
				targetHrefs.push(new URL(href, url).href);
			}
		});
	}

	const uniqueUrls = Array.from(new Set(targetHrefs));
	let totalInserted = 0;

	for (const url of uniqueUrls) {
		const res = await fetch(url);
		if (!res.ok) continue;
		const html = await res.text();
		const $ = cheerio.load(html);

		const preTags = $("pre");

		type Record = {
			congress: number;
			public_law_number: string;
			pub_law_sec: string | null;
			usc_title: string | null;
			usc_section: string | null;
			description: string | null;
		};
		const records: Record[] = [];

		let currentPl = "";
		let congress = 0;

		const urlMatch = url.match(/tbl(\d+)pl/);
		if (urlMatch) {
			congress = Number.parseInt(urlMatch[1], 10);
		}

		if (preTags.length === 0) continue;

		const text = $(preTags[0]).text();
		const lines = text.split("\n");

		let started = false;

		for (const line of lines) {
			if (line.includes("-------------")) {
				started = true;
				continue;
			}
			if (!started) continue;

			if (line.trim().length === 0) continue;

			const titleSlice = line.slice(0, 6).trim();
			const sectionSlice = line.slice(6, 19).trim();
			const descSlice = line.slice(19, 36).trim();
			const pubLawSlice = line.slice(36, 45).trim();
			const secSlice = line.slice(45, 66).trim();

			if (pubLawSlice.length > 0) {
				const parts = pubLawSlice.split("-");
				if (parts.length === 2) {
					currentPl = pubLawSlice;
					congress = Number.parseInt(parts[0], 10) || congress;
				}
			}

			if (!titleSlice) continue;
			if (!currentPl || !congress) continue;

			records.push({
				congress,
				public_law_number: currentPl,
				pub_law_sec: secSlice || null,
				usc_title: titleSlice || null,
				usc_section: sectionSlice || null,
				description: descSlice || null,
			});
		}

		if (records.length > 0) {
			const BATCH_SIZE = 100;
			for (let i = 0; i < records.length; i += BATCH_SIZE) {
				const batch = records.slice(i, i + BATCH_SIZE);
				const statements = batch.map((r) =>
					c.env.DB.prepare(
						`INSERT INTO usc_classifications (congress, public_law_number, pub_law_sec, usc_title, usc_section, description)
						 VALUES (?, ?, ?, ?, ?, ?)
						 ON CONFLICT (congress, public_law_number, pub_law_sec, usc_title, usc_section) DO UPDATE SET description=excluded.description`,
					).bind(
						r.congress,
						r.public_law_number,
						r.pub_law_sec,
						r.usc_title,
						r.usc_section,
						r.description,
					),
				);
				await c.env.DB.batch(statements);
				totalInserted += batch.length;
			}
		}
	}

	return c.json({
		status: "ok",
		totalInserted,
		urlsAnalyzed: uniqueUrls.length,
	});
});

// ──────────────────────────────────────────────────────────────
// Health & admin
// ──────────────────────────────────────────────────────────────

app.get("/", (c) => {
	return c.json({ status: "ok", service: "fastlaw-ingest" });
});

app.get("/api/versions", async (c) => {
	const { results } = await c.env.DB.prepare(`
		SELECT sv.*, s.id as source_code, s.name as source_name
		FROM source_versions sv
		JOIN sources s ON sv.source_id = s.id
		ORDER BY sv.created_at DESC
		LIMIT 50
	`).all();
	return c.json({ versions: results });
});

const INGEST_JOB_COLUMNS = `
	id, source_code, source_version_id, status,
	total_titles, processed_titles, total_nodes, processed_nodes,
	error_count, last_error,
	started_at, completed_at, created_at, updated_at`;

const ABORTABLE_JOB_STATUSES = new Set(["planning", "running"]);
const TERMINAL_JOB_STATUSES = new Set([
	"completed",
	"completed_with_errors",
	"failed",
	"aborted",
]);

function isSearchExportConfigured(env: Env): boolean {
	return Boolean(
		env.SEARCH_EXPORT_WORKFLOW &&
			env.QUICKWIT_INDEXER_URL &&
			env.QUICKWIT_WEBHOOK_SECRET &&
			env.R2_ACCOUNT_ID &&
			env.R2_ACCESS_KEY_ID &&
			env.R2_SECRET_ACCESS_KEY &&
			env.SEARCH_EXPORT_R2_BUCKET,
	);
}

async function isJobAborted(db: D1Database, jobId: string): Promise<boolean> {
	const row = await db
		.prepare("SELECT status FROM ingest_jobs WHERE id = ?")
		.bind(jobId)
		.first<{ status: string }>();
	return row?.status === "aborted";
}

async function maybeStartSearchExportWorkflow(args: {
	env: Env;
	sourceId: string;
	sourceVersionId: string;
	context: string;
}): Promise<void> {
	if (!isSearchExportConfigured(args.env)) {
		console.log(
			`[Worker] Search export skipped for ${args.context}: missing configuration.`,
		);
		return;
	}

	const instance = await args.env.SEARCH_EXPORT_WORKFLOW.create({
		params: {
			sourceId: args.sourceId,
			sourceVersionId: args.sourceVersionId,
		},
	});
	console.log(
		`[Worker] Search export workflow started for ${args.context}. instanceId=${instance.id}`,
	);
}

app.get("/api/ingest/jobs", async (c) => {
	const limit = Number.parseInt(c.req.query("limit") ?? "100", 10);
	const { results } = await c.env.DB.prepare(
		`SELECT ${INGEST_JOB_COLUMNS}
		FROM ingest_jobs
		ORDER BY created_at DESC
		LIMIT ?`,
	)
		.bind(limit)
		.all();
	return c.json({ jobs: results });
});

app.get("/api/ingest/jobs/:jobId", async (c) => {
	const job = await c.env.DB.prepare(
		`SELECT ${INGEST_JOB_COLUMNS}
		FROM ingest_jobs
		WHERE id = ?`,
	)
		.bind(c.req.param("jobId"))
		.first();

	if (!job) return c.json({ error: "Job not found" }, 404);
	return c.json({ job });
});

app.get("/api/ingest/jobs/:jobId/units", async (c) => {
	const { results } = await c.env.DB.prepare(
		`SELECT id, job_id, unit_id, status, total_nodes, processed_nodes,
			error, started_at, completed_at
		FROM ingest_job_units
		WHERE job_id = ?
		ORDER BY id`,
	)
		.bind(c.req.param("jobId"))
		.all();
	return c.json({ units: results });
});

app.post("/api/ingest/jobs/:jobId/abort", async (c) => {
	const jobId = c.req.param("jobId");
	const job = await c.env.DB.prepare(
		`SELECT ${INGEST_JOB_COLUMNS}
		FROM ingest_jobs
		WHERE id = ?`,
	)
		.bind(jobId)
		.first<{ status: string }>();

	if (!job) {
		return c.json({ error: "Job not found" }, 404);
	}
	if (TERMINAL_JOB_STATUSES.has(job.status)) {
		return c.json(
			{ error: `Job cannot be aborted from status '${job.status}'` },
			409,
		);
	}
	if (!ABORTABLE_JOB_STATUSES.has(job.status)) {
		return c.json(
			{ error: `Job status '${job.status}' is not abortable` },
			409,
		);
	}

	await c.env.DB.batch([
		c.env.DB.prepare(
			`UPDATE ingest_jobs
				SET status = 'aborted', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
				WHERE id = ? AND status IN ('planning', 'running')`,
		).bind(jobId),
		c.env.DB.prepare(
			`UPDATE ingest_job_units
				SET
					status = 'aborted',
					error = COALESCE(error, 'aborted by user'),
					completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)
				WHERE job_id = ? AND status IN ('pending', 'running')`,
		).bind(jobId),
	]);

	const updatedJob = await c.env.DB.prepare(
		`SELECT ${INGEST_JOB_COLUMNS}
		FROM ingest_jobs
		WHERE id = ?`,
	)
		.bind(jobId)
		.first();

	return c.json({ ok: true, job: updatedJob });
});

app.get("/api/storage/objects", async (c) => {
	const prefix = c.req.query("prefix");
	const limit = Number.parseInt(c.req.query("limit") ?? "100", 10);
	const cursor = c.req.query("cursor");
	const result = await c.env.STORAGE.list({ prefix, limit, cursor });
	const cursorValue = "cursor" in result ? result.cursor : null;
	return c.json({
		objects: result.objects.map((obj) => ({
			key: obj.key,
			size: obj.size,
			etag: obj.etag,
			uploaded: obj.uploaded,
		})),
		truncated: result.truncated,
		cursor: cursorValue,
	});
});

app.get("/api/diff/:oldVersionId/:newVersionId", async (c) => {
	const diff = await computeDiff(
		c.env.DB,
		c.req.param("oldVersionId"),
		c.req.param("newVersionId"),
	);
	return c.json({ diff });
});

app.post("/api/admin/reset", async (c) => {
	let cursor: string | undefined;
	do {
		const listResult = await c.env.STORAGE.list({ limit: 1000, cursor });
		if (listResult.objects.length > 0) {
			await c.env.STORAGE.delete(listResult.objects.map((obj) => obj.key));
		}
		cursor = "cursor" in listResult ? listResult.cursor : undefined;
	} while (cursor);

	await c.env.DB.batch([
		c.env.DB.prepare("DELETE FROM ingest_job_units"),
		c.env.DB.prepare("DELETE FROM ingest_jobs"),
		c.env.DB.prepare("DELETE FROM nodes"),
		c.env.DB.prepare("DELETE FROM blobs"),
		c.env.DB.prepare("DELETE FROM source_versions"),
		c.env.DB.prepare("DELETE FROM sources"),
	]);

	return c.json({ status: "ok" });
});

// ──────────────────────────────────────────────────────────────
// Generic ingest via containers
// ──────────────────────────────────────────────────────────────

app.post("/api/ingest/:source", async (c) => {
	try {
		const sourceCode = c.req.param("source");

		// Validate source code
		if (!validateSourceCode(sourceCode)) {
			return c.json({ error: `Invalid source: ${sourceCode}` }, 400);
		}

		const config = getSourceConfig(sourceCode);
		if (!config) {
			return c.json(
				{ error: `Source configuration not found: ${sourceCode}` },
				500,
			);
		}

		const unitSelectors = (c.req.query("units") ?? "")
			.split(",")
			.map((v) => v.trim())
			.filter((v) => v.length > 0);
		const manualStartUrl = c.req.query("startUrl")?.trim();

		const sourceId = await getOrCreateSource(
			c.env.DB,
			sourceCode,
			config.name,
			config.jurisdiction,
			config.region,
			config.doc_type,
		);

		const jobId = await createIngestJob(c.env.DB, sourceCode);

		// In local dev, containers run in Docker and can't reach the host via
		// localhost. Replace with host.docker.internal so callbacks work.
		const origin = new URL(c.req.url).origin;
		const callbackBase = origin.replace(
			/localhost|127\.0\.0\.1/,
			"host.docker.internal",
		);
		// sourceVersionId is unknown at this point, so we omit/leave empty in token
		const callbackToken = await signCallbackToken(
			{ jobId, sourceId },
			c.env.CALLBACK_SECRET,
		);

		const container = c.env.INGEST_CONTAINER.getByName("ingest");
		// Using "ingest" as a stable name for the service
		// We pass selectors but NO units. The container will discover.
		await container.registerJob(jobId);
		await container
			.fetch(
				new Request("http://container/ingest", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						source: sourceCode,
						sourceId,
						selectors: unitSelectors.length > 0 ? unitSelectors : undefined,
						manualStartUrl:
							sourceCode === "uspl"
								? c.env.GOVINFO_API_KEY
								: manualStartUrl && manualStartUrl.length > 0
									? manualStartUrl
									: undefined,
						callbackBase,
						callbackToken,
					}),
				}),
			)
			.then(async (res) => {
				if (!res.ok) {
					console.error(
						`Container returned ${res.status}: ${await res.text()}`,
					);
					await container.requestStopForJob(jobId);
				}
			})
			.catch(async (err) => {
				console.error("Container fetch failed:", err);
				await container.requestStopForJob(jobId);
			});

		return c.json({
			jobId,
			sourceCode,
			status: "planning", // Container is planning/discovering
		});
	} catch (error) {
		console.error(`Ingest start failed for ${c.req.param("source")}:`, error);
		return c.json({ error: "Ingest start failed" }, 500);
	}
});

app.post("/api/callback/ensureSourceVersion", async (c) => {
	const token = extractBearerToken(c.req.raw);
	const params = await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	if (await isJobAborted(c.env.DB, params.jobId)) {
		return c.json({ error: "Job aborted" }, 409);
	}
	const { sourceId, sourceVersionId, rootNode, units } = await c.req.json<{
		sourceId: string;
		sourceVersionId: string;
		rootNode: NodeMeta;
		units: Array<{ id: string; title_num: string; url: string }>;
	}>();

	console.log(
		`[Worker] ensureSourceVersion callback. jobId=${params.jobId}, svid=${sourceVersionId}, units=${units.length}`,
	);

	// ensureSourceVersion takes (db, sourceId, versionDate, rootNodeId).
	// Rust sends `usc-{version_id}`, so we extract the version suffix.
	const versionDate = sourceVersionId.replace(`${sourceId}-`, "");

	try {
		await ensureSourceVersion(c.env.DB, sourceId, versionDate, rootNode.id);

		// Insert root node
		await insertNodes(c.env.DB, [
			{
				...rootNode,
				source_version_id: sourceVersionId,
				blob_hash: null,
			},
		]);

		// Create job units and complete planning
		await completePlanning(
			c.env.DB,
			params.jobId,
			sourceVersionId,
			units.length,
		);
		await createJobUnits(
			c.env.DB,
			params.jobId,
			units.map((u) => u.id),
		);
	} catch (err) {
		console.error(`[Worker] Failed in ensureSourceVersion: ${err}`);
		return c.json({ error: String(err) }, 500);
	}

	if (units.length === 0) {
		c.executionCtx.waitUntil(
			maybeStartSearchExportWorkflow({
				env: c.env,
				sourceId,
				sourceVersionId,
				context: `sourceVersion=${sourceVersionId}`,
			}).catch((error) => {
				console.error(
					`[Worker] Search export workflow start failed for ${sourceVersionId}:`,
					error,
				);
			}),
		);
	}

	return c.json({ ok: true });
});

// ──────────────────────────────────────────────────────────────
app.post("/api/callback/containerLog", async (c) => {
	const token = extractBearerToken(c.req.raw);
	const params = await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	const payload = await c.req.json<{
		level?: "debug" | "info" | "warn" | "error";
		message: string;
		context?: Record<string, unknown>;
	}>();
	const level = payload.level ?? "info";
	const context = payload.context ? ` ${JSON.stringify(payload.context)}` : "";
	const line = `[Container][${level}][job=${params.jobId}] ${payload.message}${context}`;

	if (level === "error") {
		console.error(line);
	} else if (level === "warn") {
		console.warn(line);
	} else {
		console.log(line);
	}

	return c.json({ ok: true });
});

app.post("/api/callback/ingestError", async (c) => {
	const token = extractBearerToken(c.req.raw);
	const params = await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	const { error } = await c.req.json<{ error: string }>();

	console.error(
		`[Worker] ingestError callback received. jobId=${params.jobId}, error=${error}`,
	);
	await recordTitleError(c.env.DB, params.jobId, error, true);

	const container = c.env.INGEST_CONTAINER.getByName("ingest");
	const result = await container.requestStopForJob(params.jobId);
	console.log(
		`[Worker] ingestError callback processed. jobId=${params.jobId}, stopped=${result.stopped}, remainingJobs=${result.runningJobs.length}`,
	);

	return c.json({ ok: true });
});

app.post("/api/callback/containerStop", async (c) => {
	const token = extractBearerToken(c.req.raw);
	const params = await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	const body = await c.req
		.json<{ reason?: string }>()
		.catch((): { reason?: string } => ({}));
	const reason = body.reason ?? "unspecified";

	console.log(
		`[Worker] containerStop callback received. jobId=${params.jobId}, reason=${reason}`,
	);

	const container = c.env.INGEST_CONTAINER.getByName("ingest");
	const result = await container.requestStopForJob(params.jobId);
	console.log(
		`[Worker] containerStop callback processed. jobId=${params.jobId}, stopped=${result.stopped}, remainingJobs=${result.runningJobs.length}`,
	);

	return c.json({ ok: true });
});

// ──────────────────────────────────────────────────────────────
app.post("/api/callback/unitStart", async (c) => {
	const token = extractBearerToken(c.req.raw);
	const params = await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	if (await isJobAborted(c.env.DB, params.jobId)) {
		return c.json({ error: "Job aborted" }, 409);
	}
	const { unitId, totalNodes } = await c.req.json<{
		unitId: string;
		totalNodes: number;
	}>();

	console.log(
		`[Worker] Unit ${unitId} start callback received. jobId=${params.jobId}, totalNodes=${totalNodes}`,
	);
	await markUnitRunning(c.env.DB, params.jobId, unitId, totalNodes);
	console.log(
		`Unit ${unitId} started for job ${params.jobId}: ${totalNodes} nodes`,
	);
	return c.json({ ok: true });
});

app.post("/api/callback/insertNodeBatch", async (c) => {
	const token = extractBearerToken(c.req.raw);
	const params = await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	if (await isJobAborted(c.env.DB, params.jobId)) {
		return c.json({ error: "Job aborted" }, 409);
	}
	const { unitId, nodes } = await c.req.json<{
		unitId: string;
		nodes: NodePayload[];
	}>();

	console.log(
		`[Worker] insertNodeBatch callback received. unitId=${unitId}, count=${nodes.length}`,
	);
	if (nodes.length > 0) {
		console.log(`[Worker] Sample node ID: ${nodes[0].meta.id}`);
	}
	const allBlobs: Array<{ hashHex: string; content: string }> = [];
	const nodeInserts: IngestNode[] = [];

	for (const node of nodes) {
		let blobHash: string | null = null;

		if (node.content) {
			const contentStr = JSON.stringify(node.content);
			const bytes = new TextEncoder().encode(contentStr);
			blobHash = hash64ToHex(await hash64(bytes));
			allBlobs.push({ hashHex: blobHash, content: contentStr });
		}

		nodeInserts.push({
			...node.meta,
			source_version_id:
				node.meta.source_version_id || params.sourceVersionId || "",
			blob_hash: blobHash,
		});
	}

	await insertNodes(c.env.DB, nodeInserts);
	await incrementUnitProcessedNodes(
		c.env.DB,
		params.jobId,
		unitId,
		nodes.length,
	);

	if (allBlobs.length > 0) {
		const packfileDO = c.env.PACKFILE_DO.get(
			c.env.PACKFILE_DO.idFromName(params.sourceId),
		);
		c.executionCtx.waitUntil(
			packfileDO.appendBlobs(params.sourceId, params.sourceId, allBlobs),
		);
	}

	return c.json({ accepted: nodes.length });
});

app.post("/api/callback/progress", async (c) => {
	const token = extractBearerToken(c.req.raw);
	const params = await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	if (await isJobAborted(c.env.DB, params.jobId)) {
		return c.json({ error: "Job aborted" }, 409);
	}
	const { unitId, status, error } = await c.req.json<{
		unitId: string;
		status: string;
		error?: string;
	}>();

	if (status === "completed" || status === "skipped") {
		await markUnitCompleted(c.env.DB, params.jobId, unitId, status);
		await incrementProcessedTitles(c.env.DB, params.jobId, 1);
		console.log(`Title ${unitId} ${status} for job ${params.jobId}`);
	} else if (error) {
		await markUnitCompleted(c.env.DB, params.jobId, unitId, "error", error);
		await recordTitleError(c.env.DB, params.jobId, error, false);
		await incrementProcessedTitles(c.env.DB, params.jobId, 1);
		console.error(`Title ${unitId} failed for job ${params.jobId}: ${error}`);
	}

	if (status === "completed" || status === "skipped" || error) {
		const packfileDO = c.env.PACKFILE_DO.get(
			c.env.PACKFILE_DO.idFromName(params.sourceId),
		);
		c.executionCtx.waitUntil(
			packfileDO.flush(params.sourceId, params.sourceId),
		);
	}

	if (status === "completed" || status === "skipped") {
		const job = await c.env.DB.prepare(
			`SELECT source_version_id, status
			FROM ingest_jobs
			WHERE id = ?`,
		)
			.bind(params.jobId)
			.first<{ source_version_id: string | null; status: string }>();

		if (job?.status === "completed" && job.source_version_id) {
			c.executionCtx.waitUntil(
				maybeStartSearchExportWorkflow({
					env: c.env,
					sourceId: params.sourceId,
					sourceVersionId: job.source_version_id,
					context: `job=${params.jobId}`,
				}).catch((workflowError) => {
					console.error(
						`[Worker] Search export workflow start failed for job ${params.jobId}:`,
						workflowError,
					);
				}),
			);
		}
	}

	return c.json({ ok: true });
});

// ──────────────────────────────────────────────────────────────
// Generic R2 cache proxy
// ──────────────────────────────────────────────────────────────

const CACHE_R2_PREFIX = "cache/";
const R2_MULTIPART_PART_SIZE = 8 * 1024 * 1024;
const requestRateLimiters = new Map<
	number,
	{ nextAllowedAtMs: number; throttleChain: Promise<void> }
>();

async function throttleRequestsPerSecond(
	requestsPerSecond?: number,
): Promise<void> {
	if (requestsPerSecond === undefined) return;
	const intervalMs = 1000 / requestsPerSecond;
	const limiterKey = Math.round(intervalMs);
	const limiter = requestRateLimiters.get(limiterKey) ?? {
		nextAllowedAtMs: 0,
		throttleChain: Promise.resolve(),
	};
	requestRateLimiters.set(limiterKey, limiter);

	const scheduled = limiter.throttleChain.then(async () => {
		const now = Date.now();
		const waitMs = Math.max(0, limiter.nextAllowedAtMs - now);
		if (waitMs > 0) {
			await new Promise<void>((resolve) => {
				setTimeout(resolve, waitMs);
			});
		}
		limiter.nextAllowedAtMs = Date.now() + intervalMs;
	});
	limiter.throttleChain = scheduled.catch(() => undefined);
	await scheduled;
}

function concatChunks(chunks: Uint8Array[], totalSize: number): Uint8Array {
	const combined = new Uint8Array(totalSize);
	let offset = 0;
	for (const chunk of chunks) {
		combined.set(chunk, offset);
		offset += chunk.length;
	}
	return combined;
}

async function uploadMultipartFromStream(
	bucket: R2Bucket,
	key: string,
	stream: ReadableStream<Uint8Array>,
): Promise<number> {
	const upload = await bucket.createMultipartUpload(key);
	type UploadedPart = Awaited<ReturnType<R2MultipartUpload["uploadPart"]>>;
	const uploadedParts: UploadedPart[] = [];
	const reader = stream.getReader();
	const pendingChunks: Uint8Array[] = [];
	let pendingSize = 0;
	let partNumber = 1;
	let totalSize = 0;

	const enqueuePending = (chunk: Uint8Array): void => {
		pendingChunks.push(chunk);
		pendingSize += chunk.byteLength;
	};

	const shiftPending = (targetSize: number): Uint8Array => {
		const partChunks: Uint8Array[] = [];
		let remaining = targetSize;

		while (remaining > 0) {
			const chunk = pendingChunks[0];
			if (!chunk) {
				throw new Error("Invariant violation: missing pending chunk");
			}
			if (chunk.byteLength <= remaining) {
				partChunks.push(chunk);
				pendingChunks.shift();
				remaining -= chunk.byteLength;
				continue;
			}

			partChunks.push(chunk.subarray(0, remaining));
			pendingChunks[0] = chunk.subarray(remaining);
			remaining = 0;
		}

		pendingSize -= targetSize;
		return concatChunks(partChunks, targetSize);
	};

	const uploadPart = async (partData: Uint8Array): Promise<void> => {
		const currentPartNumber = partNumber;
		let uploadedPart: UploadedPart;
		try {
			uploadedPart = await upload.uploadPart(currentPartNumber, partData);
		} catch (error) {
			const message =
				error instanceof Error
					? error.message
					: "unknown multipart uploadPart error";
			throw new Error(
				`Multipart upload failed for ${key} at part ${currentPartNumber} (${partData.byteLength} bytes, ${totalSize} bytes uploaded so far): ${message}`,
			);
		}
		uploadedParts.push(uploadedPart);
		partNumber += 1;
		totalSize += partData.byteLength;
	};

	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) {
				break;
			}
			enqueuePending(value);
			while (pendingSize >= R2_MULTIPART_PART_SIZE) {
				await uploadPart(shiftPending(R2_MULTIPART_PART_SIZE));
			}
		}

		if (pendingSize > 0) {
			await uploadPart(shiftPending(pendingSize));
		}

		if (uploadedParts.length === 0) {
			await bucket.put(key, new Uint8Array(0));
			return 0;
		}

		try {
			await upload.complete(uploadedParts);
		} catch (error) {
			const message =
				error instanceof Error
					? error.message
					: "unknown multipart complete error";
			throw new Error(
				`Multipart completion failed for ${key} after ${uploadedParts.length} parts and ${totalSize} bytes: ${message}`,
			);
		}
		return totalSize;
	} catch (error) {
		const abortError = await upload.abort().catch((abortError) => abortError);
		if (abortError instanceof Error) {
			console.error(
				`Failed to abort multipart upload for ${key}: ${abortError.message}`,
			);
		}
		if (error instanceof Error) {
			console.error(error.message);
			throw error;
		}
		throw new Error(`Multipart upload failed for ${key}: unknown error`);
	} finally {
		reader.releaseLock();
	}
}

type CacheRequest = {
	url: string;
	extractZip?: boolean;
	cacheKey: string;
	throttleRequestsPerSecond?: number;
};

function getValidatedCacheRequest(payload: CacheRequest): {
	url: string;
	extractZip: boolean;
	cacheKey: string;
	throttleRequestsPerSecond?: number;
} {
	if (!payload.cacheKey) {
		throw new Error("Missing cacheKey");
	}
	if (
		payload.throttleRequestsPerSecond !== undefined &&
		(!Number.isFinite(payload.throttleRequestsPerSecond) ||
			payload.throttleRequestsPerSecond <= 0)
	) {
		throw new Error("Invalid throttleRequestsPerSecond");
	}
	return {
		url: payload.url,
		extractZip: payload.extractZip === true,
		cacheKey: payload.cacheKey,
		throttleRequestsPerSecond: payload.throttleRequestsPerSecond,
	};
}

async function populateCacheObject(
	bucket: R2Bucket,
	url: string,
	r2Key: string,
	extractZip: boolean,
	throttleRps?: number,
): Promise<{ r2Key: string; totalSize: number }> {
	await throttleRequestsPerSecond(throttleRps);

	console.log("fetching", url);
	const response = await fetch(url, {
		headers: { "User-Agent": "fastlaw-ingest/1.0" },
	});
	if (!response.ok) {
		throw new Error(`Failed to fetch ${url}: ${response.status}`);
	}
	if (!response.body) {
		throw new Error("empty_response_body");
	}

	const contentType = response.headers.get("content-type") ?? "";
	if (extractZip && contentType.toLowerCase().includes("text/html")) {
		throw new Error("html_response");
	}

	if (extractZip) {
		const totalSize = await uploadMultipartFromStream(
			bucket,
			r2Key,
			response.body,
		);
		console.log(`Cached ZIP ${url} → ${r2Key} (${totalSize} bytes, multipart)`);
		return { r2Key, totalSize };
	}

	const responseBytes = await response.arrayBuffer();
	await bucket.put(r2Key, responseBytes);
	const stored = await bucket.head(r2Key);
	const totalSize = stored?.size ?? 0;
	console.log(`Cached ${url} → ${r2Key} (${totalSize} bytes)`);
	return { r2Key, totalSize };
}

async function ensureCachedObject(
	bucket: R2Bucket,
	url: string,
	cacheKey: string,
	extractZip: boolean,
	throttleRps?: number,
): Promise<{ r2Key: string; totalSize: number }> {
	const r2Key = `${CACHE_R2_PREFIX}${cacheKey}`;
	const head = await bucket.head(r2Key);
	if (head) {
		return { r2Key, totalSize: head.size };
	}

	return populateCacheObject(bucket, url, r2Key, extractZip, throttleRps);
}

app.post("/api/proxy/cache", async (c) => {
	try {
		const token = extractBearerToken(c.req.raw);
		await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	} catch {
		return c.json({ error: "Unauthorized" }, 401);
	}

	try {
		const { url, extractZip, cacheKey, throttleRequestsPerSecond } =
			getValidatedCacheRequest(await c.req.json<CacheRequest>());
		const result = await ensureCachedObject(
			c.env.STORAGE,
			url,
			cacheKey,
			extractZip,
			throttleRequestsPerSecond,
		);
		return c.json(result);
	} catch (error) {
		if (error instanceof Error) {
			if (error.message === "Missing cacheKey") {
				return c.json({ error: error.message }, 400);
			}
			if (error.message === "Invalid throttleRequestsPerSecond") {
				return c.json({ error: error.message }, 400);
			}
			if (error.message === "html_response") {
				return c.json({ error: error.message }, 422);
			}
			if (error.message === "empty_response_body") {
				return c.json({ error: error.message }, 502);
			}
			if (error.message.startsWith("Failed to fetch")) {
				return c.json({ error: error.message }, 502);
			}
			return c.json({ error: error.message }, 500);
		}
		return c.json({ error: "cache_proxy_failed" }, 500);
	}
});

app.post("/api/proxy/cache-read", async (c) => {
	try {
		const token = extractBearerToken(c.req.raw);
		await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	} catch {
		return c.json({ error: "Unauthorized" }, 401);
	}

	try {
		const { url, extractZip, cacheKey, throttleRequestsPerSecond } =
			getValidatedCacheRequest(await c.req.json<CacheRequest>());
		const { r2Key } = await ensureCachedObject(
			c.env.STORAGE,
			url,
			cacheKey,
			extractZip,
			throttleRequestsPerSecond,
		);
		const obj = await c.env.STORAGE.get(r2Key);
		if (!obj) {
			return c.json({ error: `Object not found: ${r2Key}` }, 404);
		}
		return new Response(obj.body, {
			headers: {
				"Content-Type": "application/octet-stream",
				"X-Cache-Key": r2Key,
			},
		});
	} catch (error) {
		if (error instanceof Error) {
			if (error.message === "Missing cacheKey") {
				return c.json({ error: error.message }, 400);
			}
			if (error.message === "Invalid throttleRequestsPerSecond") {
				return c.json({ error: error.message }, 400);
			}
			if (error.message === "html_response") {
				return c.json({ error: error.message }, 422);
			}
			if (error.message === "empty_response_body") {
				return c.json({ error: error.message }, 502);
			}
			if (error.message.startsWith("Failed to fetch")) {
				return c.json({ error: error.message }, 502);
			}
			return c.json({ error: error.message }, 500);
		}
		return c.json({ error: "cache_read_failed" }, 500);
	}
});

app.get("/api/proxy/r2-read", async (c) => {
	try {
		const token = extractBearerToken(c.req.raw);
		await verifyCallbackToken(token, c.env.CALLBACK_SECRET);
	} catch {
		return c.json({ error: "Unauthorized" }, 401);
	}

	const key = c.req.query("key");
	const offsetRaw = c.req.query("offset");
	const lengthRaw = c.req.query("length");

	if (!key) {
		return c.json({ error: "Missing key" }, 400);
	}

	const obj =
		lengthRaw == null
			? await c.env.STORAGE.get(key)
			: await c.env.STORAGE.get(key, {
					range: {
						offset: Number(offsetRaw ?? "0"),
						length: Number(lengthRaw),
					},
				});

	if (!obj) {
		return c.json({ error: `Object not found: ${key}` }, 404);
	}

	return new Response(obj.body, {
		headers: { "Content-Type": "application/octet-stream" },
	});
});

// ──────────────────────────────────────────────────────────────
// Vector workflow
// ──────────────────────────────────────────────────────────────

app.post("/api/ingest/vector/workflow", async (c) => {
	try {
		const body = await c.req
			.json<VectorWorkflowParams>()
			.catch(() => ({}) as VectorWorkflowParams);
		const instance = await c.env.VECTOR_WORKFLOW.create({
			params: {
				force: body.force,
				sourceId: body.sourceId,
				sourceVersionId: body.sourceVersionId,
				batchSize: body.batchSize,
			},
		});
		return c.json({ instanceId: instance.id, status: await instance.status() });
	} catch (error) {
		console.error("Vector workflow creation failed:", error);
		return c.json({ error: "Vector workflow creation failed" }, 500);
	}
});

app.get("/api/ingest/vector/workflow/:instanceId", async (c) => {
	try {
		const instance = await c.env.VECTOR_WORKFLOW.get(c.req.param("instanceId"));
		return c.json({ instanceId: instance.id, status: await instance.status() });
	} catch (error) {
		console.error("Vector workflow status failed:", error);
		return c.json({ error: "Vector workflow status failed" }, 500);
	}
});

// ──────────────────────────────────────────────────────────────
// Search export workflow
// ──────────────────────────────────────────────────────────────

app.post("/api/ingest/search-export/workflow", async (c) => {
	try {
		const body = await c.req
			.json<SearchExportWorkflowParams>()
			.catch(() => ({}) as SearchExportWorkflowParams);
		const instance = await c.env.SEARCH_EXPORT_WORKFLOW.create({
			params: {
				force: body.force,
				sourceId: body.sourceId,
				sourceVersionId: body.sourceVersionId,
				batchSize: body.batchSize,
			},
		});
		return c.json({ instanceId: instance.id, status: await instance.status() });
	} catch (error) {
		console.error("Search export workflow creation failed:", error);
		return c.json({ error: "Search export workflow creation failed" }, 500);
	}
});

app.get("/api/ingest/search-export/workflow/:instanceId", async (c) => {
	try {
		const instance = await c.env.SEARCH_EXPORT_WORKFLOW.get(
			c.req.param("instanceId"),
		);
		return c.json({ instanceId: instance.id, status: await instance.status() });
	} catch (error) {
		console.error("Search export workflow status failed:", error);
		return c.json({ error: "Search export workflow status failed" }, 500);
	}
});

// ──────────────────────────────────────────────────────────────
// Test cache UI
// ──────────────────────────────────────────────────────────────

app.get("/api/test-cache", async (c) => {
	const token = await signCallbackToken(
		{ jobId: "test", sourceId: "test", sourceVersionId: "" },
		c.env.CALLBACK_SECRET,
	);
	const baseUrl = new URL(c.req.url).origin;

	return c.html(/* html */ `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>Cache Proxy Tester</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 720px; margin: 2rem auto; padding: 0 1rem; }
  label { display: block; margin-top: 1rem; font-weight: 600; }
  input, select { width: 100%; padding: 0.4rem; margin-top: 0.25rem; box-sizing: border-box; font-family: monospace; }
  button { margin-top: 1rem; padding: 0.5rem 1.5rem; }
  pre { background: #f4f4f4; padding: 1rem; overflow-x: auto; white-space: pre-wrap; word-break: break-all; }
  .meta { color: #666; font-size: 0.85rem; }
</style>
</head>
<body>
<h1>Cache Proxy Tester</h1>

<label>URL to fetch &amp; cache
  <input id="url" placeholder="https://example.com/file.xml" />
</label>

<label>Cache key
  <input id="cacheKey" placeholder="test/my-key" />
</label>

<label>Throttle (requests/sec, optional)
  <input id="throttle" type="number" placeholder="e.g. 33" />
</label>

<label>Endpoint
  <select id="endpoint">
    <option value="cache">POST /api/proxy/cache (metadata only)</option>
    <option value="cache-read">POST /api/proxy/cache-read (return body)</option>
  </select>
</label>

<button onclick="doFetch()">Fetch</button>

<h2>Response</h2>
<p class="meta" id="meta"></p>
<pre id="output">(nothing yet)</pre>

<script>
const TOKEN = ${JSON.stringify(token)};
const BASE = ${JSON.stringify(baseUrl)};

async function doFetch() {
  const url = document.getElementById("url").value.trim();
  const cacheKey = document.getElementById("cacheKey").value.trim();
  const throttle = document.getElementById("throttle").value.trim();
  const endpoint = document.getElementById("endpoint").value;

  if (!url || !cacheKey) { alert("URL and cache key are required"); return; }

  const body = { url, cacheKey };
  if (throttle) body.throttleRequestsPerSecond = Number(throttle);

  document.getElementById("output").textContent = "Loading…";
  document.getElementById("meta").textContent = "";

  try {
    const resp = await fetch(BASE + "/api/proxy/" + endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + TOKEN,
      },
      body: JSON.stringify(body),
    });

    const ct = resp.headers.get("content-type") || "";
    const status = resp.status + " " + resp.statusText;
    const cacheHeader = resp.headers.get("x-cache-key") || "";
    document.getElementById("meta").textContent =
      status + (cacheHeader ? "  •  X-Cache-Key: " + cacheHeader : "");

    if (ct.includes("json")) {
      document.getElementById("output").textContent = JSON.stringify(await resp.json(), null, 2);
    } else {
      const text = await resp.text();
      document.getElementById("output").textContent = text.slice(0, 200_000);
    }
  } catch (e) {
    document.getElementById("output").textContent = "Error: " + e.message;
  }
}
</script>
</body>
</html>`);
});

// ──────────────────────────────────────────────────────────────
// Export
// ──────────────────────────────────────────────────────────────

const worker: ExportedHandler<Env> = {
	fetch: app.fetch,
	queue() {},
};

export default worker;
export { IngestContainer, PackfileDO, VectorIngestWorkflow };
