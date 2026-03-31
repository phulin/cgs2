import {
	WorkflowEntrypoint,
	type WorkflowEvent,
	type WorkflowStep,
} from "cloudflare:workers";
import type {
	Env,
	SearchExportWorkflowParams,
	SearchExportWorkflowResult,
} from "../../types";
import { readBlobJson } from "../packfile";
import {
	buildSearchText,
	buildTitleText,
	createBreadcrumbBuilder,
	extractContentText,
	type BreadcrumbNode,
	type StoredNodeContent,
} from "./document";
import { presignR2GetUrl } from "./presign";

const DEFAULT_BATCH_SIZE = 250;
const MAX_BATCH_SIZE = 1000;
const EXPORT_URL_TTL_SECONDS = 3600;

interface SearchSourceVersionRow {
	source_id: string;
	source_name: string;
	jurisdiction: string;
	region: string;
	doc_type: string;
	source_version_id: string;
	version_date: string;
}

interface SearchNodeRow extends BreadcrumbNode {
	source_version_id: string;
	level_name: string;
	level_index: number;
	sort_order: number;
	path: string | null;
	readable_id: string | null;
	source_url: string | null;
	accessed_at: string | null;
	blob_hash: string | null;
	blob_packfile_key: string | null;
	blob_offset: number | null;
	blob_size: number | null;
}

interface SearchExportManifest {
	schema_version: number;
	export_id: string;
	source_id: string;
	source_version_id: string;
	version_date: string;
	doc_count: number;
	blob_path: string;
	generated_at: string;
}

interface SearchDocument {
	doc_id: string;
	source_id: string;
	source_name: string;
	jurisdiction: string;
	region: string;
	doc_type: string;
	source_version_id: string;
	version_date: string;
	node_id: string;
	parent_id: string | null;
	level_name: string;
	level_index: number;
	sort_order: number;
	path: string | null;
	readable_id: string | null;
	heading_citation: string | null;
	name: string | null;
	source_url: string | null;
	accessed_at: string | null;
	blob_hash: string | null;
	has_body: boolean;
	breadcrumb: string;
	title_text: string;
	body_text: string;
	search_text: string;
}

function getBatchSize(value: number | undefined): number {
	if (!value || Number.isNaN(value)) {
		return DEFAULT_BATCH_SIZE;
	}
	return Math.max(1, Math.min(MAX_BATCH_SIZE, Math.floor(value)));
}

function buildExportPaths(sourceId: string, sourceVersionId: string) {
	const basePath = `search-exports/${sourceId}/${sourceVersionId}`;
	return {
		basePath,
		exportKey: `${basePath}/nodes.ndjson`,
		manifestKey: `${basePath}/manifest.json`,
	};
}

function hasSearchExportConfig(env: Env): boolean {
	return Boolean(
		env.QUICKWIT_INDEXER_URL &&
			env.QUICKWIT_WEBHOOK_SECRET &&
			env.R2_ACCOUNT_ID &&
			env.R2_ACCESS_KEY_ID &&
			env.R2_SECRET_ACCESS_KEY &&
			env.SEARCH_EXPORT_R2_BUCKET,
	);
}

async function hmacHex(secret: string, payload: string): Promise<string> {
	const key = await crypto.subtle.importKey(
		"raw",
		new TextEncoder().encode(secret),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);
	const sig = await crypto.subtle.sign(
		"HMAC",
		key,
		new TextEncoder().encode(payload),
	);
	return [...new Uint8Array(sig)]
		.map((byte) => byte.toString(16).padStart(2, "0"))
		.join("");
}

async function resolveSourceVersion(args: {
	env: Env;
	sourceVersionId?: string;
	sourceId?: string;
	force?: boolean;
}): Promise<
	| {
			skipped: true;
			reason: string;
	  }
	| {
			skipped: false;
			source: SearchSourceVersionRow;
	  }
> {
	const { env, sourceVersionId, sourceId, force } = args;
	if (!sourceVersionId && !sourceId) {
		throw new Error("Search export requires sourceVersionId or sourceId.");
	}

	if (sourceVersionId) {
		const selected = await env.DB.prepare(
			`SELECT
				s.id as source_id,
				s.name as source_name,
				s.jurisdiction,
				s.region,
				s.doc_type,
				sv.id as source_version_id,
				sv.version_date
			FROM source_versions sv
			JOIN sources s ON s.id = sv.source_id
			WHERE sv.id = ?
			LIMIT 1`,
		)
			.bind(sourceVersionId)
			.first<SearchSourceVersionRow>();
		if (!selected) {
			throw new Error(`Unknown source version: ${sourceVersionId}`);
		}

		if (force) {
			return { skipped: false, source: selected };
		}

		const latest = await env.DB.prepare(
			`SELECT id
			FROM source_versions
			WHERE source_id = ?
			ORDER BY version_date DESC
			LIMIT 1`,
		)
			.bind(selected.source_id)
			.first<{ id: string }>();
		if (!latest) {
			throw new Error(`No latest source version found for ${selected.source_id}`);
		}
		if (latest.id !== sourceVersionId) {
			return {
				skipped: true,
				reason: `Source version ${sourceVersionId} is not latest for ${selected.source_id}.`,
			};
		}
		return { skipped: false, source: selected };
	}

	const latest = await env.DB.prepare(
		`SELECT
			s.id as source_id,
			s.name as source_name,
			s.jurisdiction,
			s.region,
			s.doc_type,
			sv.id as source_version_id,
			sv.version_date
		FROM source_versions sv
		JOIN sources s ON s.id = sv.source_id
		WHERE sv.source_id = ?
		ORDER BY sv.version_date DESC
		LIMIT 1`,
	)
		.bind(sourceId)
		.first<SearchSourceVersionRow>();
	if (!latest) {
		throw new Error(`No source version found for source ${sourceId}`);
	}
	return { skipped: false, source: latest };
}

async function loadBreadcrumbNodes(
	env: Env,
	sourceVersionId: string,
): Promise<BreadcrumbNode[]> {
	const result = await env.DB.prepare(
		`SELECT id, parent_id, heading_citation, name
		FROM nodes
		WHERE source_version_id = ?
		ORDER BY level_index ASC, sort_order ASC, id ASC`,
	)
		.bind(sourceVersionId)
		.all<BreadcrumbNode>();
	return result.results;
}

async function countNodes(env: Env, sourceVersionId: string): Promise<number> {
	const record = await env.DB.prepare(
		`SELECT COUNT(*) as count
		FROM nodes
		WHERE source_version_id = ?`,
	)
		.bind(sourceVersionId)
		.first<{ count: number }>();
	return record?.count ?? 0;
}

async function loadNodeBatch(args: {
	env: Env;
	sourceVersionId: string;
	cursor: string | null;
	batchSize: number;
	sourceId: string;
}): Promise<SearchNodeRow[]> {
	const { env, sourceVersionId, cursor, batchSize, sourceId } = args;
	const baseQuery = `SELECT
			n.id,
			n.source_version_id,
			n.parent_id,
			n.level_name,
			n.level_index,
			n.sort_order,
			n.name,
			n.path,
			n.readable_id,
			n.heading_citation,
			n.source_url,
			n.accessed_at,
			n.blob_hash,
			b.packfile_key as blob_packfile_key,
			b.offset as blob_offset,
			b.size as blob_size
		FROM nodes n
		LEFT JOIN blobs b
			ON b.hash = n.blob_hash
			AND b.source_id = ?
		WHERE n.source_version_id = ?`;

	if (cursor) {
		const result = await env.DB.prepare(
			`${baseQuery}
			AND n.id > ?
			ORDER BY n.id ASC
			LIMIT ?`,
		)
			.bind(sourceId, sourceVersionId, cursor, batchSize)
			.all<SearchNodeRow>();
		return result.results;
	}

	const result = await env.DB.prepare(
		`${baseQuery}
		ORDER BY n.id ASC
		LIMIT ?`,
	)
		.bind(sourceId, sourceVersionId, batchSize)
		.all<SearchNodeRow>();
	return result.results;
}

async function loadBodyText(
	env: Env,
	node: SearchNodeRow,
): Promise<string> {
	if (!node.blob_hash) {
		return "";
	}
	if (
		node.blob_packfile_key == null ||
		node.blob_offset == null ||
		node.blob_size == null
	) {
		throw new Error(`Missing blob location for node ${node.id}`);
	}

	const content = await readBlobJson<StoredNodeContent>(
		env.STORAGE,
		{
			packfileKey: node.blob_packfile_key,
			offset: node.blob_offset,
			size: node.blob_size,
		},
		node.blob_hash,
	);
	return extractContentText(content);
}

async function toSearchDocument(args: {
	env: Env;
	source: SearchSourceVersionRow;
	node: SearchNodeRow;
	buildBreadcrumb: (nodeId: string) => string;
}): Promise<SearchDocument> {
	const { env, source, node, buildBreadcrumb } = args;
	const bodyText = await loadBodyText(env, node);
	const titleText = buildTitleText({
		headingCitation: node.heading_citation,
		name: node.name,
	});
	const breadcrumb = buildBreadcrumb(node.id);

	return {
		doc_id: `${source.source_version_id}:${node.id}`,
		source_id: source.source_id,
		source_name: source.source_name,
		jurisdiction: source.jurisdiction,
		region: source.region,
		doc_type: source.doc_type,
		source_version_id: source.source_version_id,
		version_date: source.version_date,
		node_id: node.id,
		parent_id: node.parent_id,
		level_name: node.level_name,
		level_index: node.level_index,
		sort_order: node.sort_order,
		path: node.path,
		readable_id: node.readable_id,
		heading_citation: node.heading_citation,
		name: node.name,
		source_url: node.source_url,
		accessed_at: node.accessed_at,
		blob_hash: node.blob_hash,
		has_body: bodyText.length > 0,
		breadcrumb,
		title_text: titleText,
		body_text: bodyText,
		search_text: buildSearchText({
			breadcrumb,
			titleText,
			bodyText,
		}),
	};
}

async function exportLatestSource(args: {
	env: Env;
	source: SearchSourceVersionRow;
	batchSize: number;
	buildBreadcrumb: (nodeId: string) => string;
	expectedDocuments: number;
}): Promise<{
	exportKey: string;
	manifestKey: string;
	exportedDocuments: number;
}> {
	const { env, source, batchSize, buildBreadcrumb, expectedDocuments } = args;
	const { exportKey, manifestKey } = buildExportPaths(
		source.source_id,
		source.source_version_id,
	);
	const encoder = new TextEncoder();
	const stream = new TransformStream<Uint8Array, Uint8Array>();
	const writer = stream.writable.getWriter();
	const uploadPromise = env.STORAGE.put(exportKey, stream.readable, {
		httpMetadata: {
			contentType: "application/x-ndjson; charset=utf-8",
		},
	});

	let cursor: string | null = null;
	let exportedDocuments = 0;

	try {
		while (true) {
			const rows = await loadNodeBatch({
				env,
				sourceVersionId: source.source_version_id,
				cursor,
				batchSize,
				sourceId: source.source_id,
			});
			if (rows.length === 0) {
				break;
			}

			const documents = await Promise.all(
				rows.map((node) =>
					toSearchDocument({
						env,
						source,
						node,
						buildBreadcrumb,
					}),
				),
			);
			for (const document of documents) {
				await writer.write(encoder.encode(`${JSON.stringify(document)}\n`));
			}

			exportedDocuments += documents.length;
			cursor = rows[rows.length - 1]?.id ?? null;
		}
	} finally {
		await writer.close();
	}

	await uploadPromise;

	if (exportedDocuments !== expectedDocuments) {
		throw new Error(
			`Search export count mismatch for ${source.source_version_id}: expected ${expectedDocuments}, wrote ${exportedDocuments}.`,
		);
	}

	const manifest: SearchExportManifest = {
		schema_version: 1,
		export_id: `${new Date().toISOString()}-${source.source_id}-${source.source_version_id}`,
		source_id: source.source_id,
		source_version_id: source.source_version_id,
		version_date: source.version_date,
		doc_count: exportedDocuments,
		blob_path: exportKey,
		generated_at: new Date().toISOString(),
	};
	await env.STORAGE.put(manifestKey, JSON.stringify(manifest, null, 2), {
		httpMetadata: {
			contentType: "application/json; charset=utf-8",
		},
	});

	return { exportKey, manifestKey, exportedDocuments };
}

async function notifyIndexer(args: {
	env: Env;
	source: SearchSourceVersionRow;
	exportKey: string;
	manifestKey: string;
	exportedDocuments: number;
}): Promise<void> {
	const {
		env,
		source,
		exportKey,
		manifestKey,
		exportedDocuments,
	} = args;
	if (!env.QUICKWIT_INDEXER_URL || !env.QUICKWIT_WEBHOOK_SECRET) {
		throw new Error("Missing Quickwit indexer webhook configuration.");
	}
	if (
		!env.R2_ACCOUNT_ID ||
		!env.R2_ACCESS_KEY_ID ||
		!env.R2_SECRET_ACCESS_KEY ||
		!env.SEARCH_EXPORT_R2_BUCKET
	) {
		throw new Error("Missing R2 presign configuration for search export.");
	}

	const expiresInSeconds = Number(
		env.QUICKWIT_EXPORT_URL_TTL_SECONDS ?? EXPORT_URL_TTL_SECONDS,
	);
	const [manifestUrl, blobUrl] = await Promise.all([
		presignR2GetUrl({
			accountId: env.R2_ACCOUNT_ID,
			accessKeyId: env.R2_ACCESS_KEY_ID,
			secretAccessKey: env.R2_SECRET_ACCESS_KEY,
			bucket: env.SEARCH_EXPORT_R2_BUCKET,
			key: manifestKey,
			expiresInSeconds,
		}),
		presignR2GetUrl({
			accountId: env.R2_ACCOUNT_ID,
			accessKeyId: env.R2_ACCESS_KEY_ID,
			secretAccessKey: env.R2_SECRET_ACCESS_KEY,
			bucket: env.SEARCH_EXPORT_R2_BUCKET,
			key: exportKey,
			expiresInSeconds,
		}),
	]);

	const timestamp = new Date().toISOString();
	const body = JSON.stringify({
		event: "source_export_ready",
		schema_version: 1,
		source_id: source.source_id,
		source_version_id: source.source_version_id,
		version_date: source.version_date,
		exported_documents: exportedDocuments,
		manifest_url: manifestUrl,
		blob_url: blobUrl,
		generated_at: timestamp,
	});
	const signature = await hmacHex(
		env.QUICKWIT_WEBHOOK_SECRET,
		`${timestamp}\n${body}`,
	);
	const response = await fetch(env.QUICKWIT_INDEXER_URL, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
			"X-Fastlaw-Timestamp": timestamp,
			"X-Fastlaw-Signature": signature,
		},
		body,
	});

	if (!response.ok) {
		const detail = await response.text();
		throw new Error(
			`Quickwit indexer webhook failed: ${response.status} ${detail}`,
		);
	}
}

export class SearchExportWorkflow extends WorkflowEntrypoint<
	Env,
	SearchExportWorkflowParams
> {
	async run(
		event: WorkflowEvent<SearchExportWorkflowParams>,
		step: WorkflowStep,
	): Promise<SearchExportWorkflowResult> {
		const resolved = await step.do("resolve-source-version", async () => {
			return await resolveSourceVersion({
				env: this.env,
				sourceId: event.payload.sourceId,
				sourceVersionId: event.payload.sourceVersionId,
				force: event.payload.force,
			});
		});

		if (resolved.skipped) {
			return {
				sourceId: event.payload.sourceId ?? "",
				sourceVersionId: event.payload.sourceVersionId ?? "",
				exportedDocuments: 0,
				expectedDocuments: 0,
				exportKey: null,
				manifestKey: null,
				notified: false,
				skipped: true,
				reason: resolved.reason,
			};
		}

		const batchSize = getBatchSize(event.payload.batchSize);
		const breadcrumbNodes = await step.do("load-breadcrumb-nodes", async () => {
			return await loadBreadcrumbNodes(
				this.env,
				resolved.source.source_version_id,
			);
		});
		const expectedDocuments = await step.do("count-documents", async () => {
			return await countNodes(this.env, resolved.source.source_version_id);
		});
		const exportResult = await step.do("export-search-blob", async () => {
			return await exportLatestSource({
				env: this.env,
				source: resolved.source,
				batchSize,
				buildBreadcrumb: createBreadcrumbBuilder(breadcrumbNodes),
				expectedDocuments,
			});
		});

		if (!hasSearchExportConfig(this.env)) {
			return {
				sourceId: resolved.source.source_id,
				sourceVersionId: resolved.source.source_version_id,
				exportedDocuments: exportResult.exportedDocuments,
				expectedDocuments,
				exportKey: exportResult.exportKey,
				manifestKey: exportResult.manifestKey,
				notified: false,
				skipped: false,
				reason: "Search export created, but Quickwit webhook configuration is missing.",
			};
		}

		await step.do("notify-quickwit-indexer", async () => {
			await notifyIndexer({
				env: this.env,
				source: resolved.source,
				exportKey: exportResult.exportKey,
				manifestKey: exportResult.manifestKey,
				exportedDocuments: exportResult.exportedDocuments,
			});
			return { ok: true };
		});

		return {
			sourceId: resolved.source.source_id,
			sourceVersionId: resolved.source.source_version_id,
			exportedDocuments: exportResult.exportedDocuments,
			expectedDocuments,
			exportKey: exportResult.exportKey,
			manifestKey: exportResult.manifestKey,
			notified: true,
			skipped: false,
			reason: null,
		};
	}
}
