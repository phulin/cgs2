import type { IngestContainer } from "./lib/ingest-container";
import type { PackfileDO } from "./lib/packfile-do";

export interface BlobLocation {
	packfileKey: string;
	offset: number;
	size: number;
}

export interface BlobEntry {
	hash: string;
	offset: number;
	size: number;
}

export interface Source {
	id: string;
	name: string;
	jurisdiction: string;
	region: string;
	doc_type: string;
}

export interface SourceVersion {
	id: string;
	source_id: string;
	version_date: string;
	root_node_id: string | null;
	created_at: string;
}

export interface NodeMeta {
	id: string;
	source_version_id: string;
	parent_id: string | null;
	level_name: string;
	level_index: number;
	sort_order: number;
	name: string | null;
	path: string | null;
	readable_id: string | null;
	heading_citation: string | null;
	source_url: string | null;
	accessed_at: string | null;
}

export interface IngestNode extends NodeMeta {
	blob_hash: string | null;
}

export type NodeInsert = IngestNode;

export interface NodePayload {
	meta: NodeMeta;
	content: unknown | null;
}

export interface DiffResult {
	added: string[];
	removed: string[];
	modified: string[];
}

export interface IngestionResult {
	sourceVersionId: string;
	nodesCreated: number;
	diff: DiffResult | null;
}

export interface VectorWorkflowParams {
	force?: boolean;
	sourceId?: string;
	sourceVersionId?: string;
	batchSize?: number;
}

export interface SearchExportWorkflowParams {
	force?: boolean;
	sourceId?: string;
	sourceVersionId?: string;
	batchSize?: number;
}

export interface SearchExportWorkflowResult {
	sourceId: string;
	sourceVersionId: string;
	exportedDocuments: number;
	expectedDocuments: number;
	exportKey: string | null;
	manifestKey: string | null;
	notified: boolean;
	skipped: boolean;
	reason: string | null;
}

export type IngestSourceCode = string;

export interface Env {
	DB: D1Database;
	STORAGE: R2Bucket;
	INGEST_CONTAINER: DurableObjectNamespace<IngestContainer>;
	PACKFILE_DO: DurableObjectNamespace<PackfileDO>;
	AI: Ai;
	VECTOR_SEARCH_INDEX: Vectorize;
	USC_DOWNLOAD_BASE: string;
	GOVINFO_API_KEY: string;
	CALLBACK_SECRET: string;
	VECTOR_WORKFLOW: Workflow<VectorWorkflowParams>;
	SEARCH_EXPORT_WORKFLOW: Workflow<SearchExportWorkflowParams>;
	QUICKWIT_INDEXER_URL?: string;
	QUICKWIT_WEBHOOK_SECRET?: string;
	QUICKWIT_EXPORT_URL_TTL_SECONDS?: string;
	R2_ACCOUNT_ID?: string;
	R2_ACCESS_KEY_ID?: string;
	R2_SECRET_ACCESS_KEY?: string;
	SEARCH_EXPORT_R2_BUCKET?: string;
	// CGA/MGL adapters (unhooked from worker, kept for future use)
	GODADDY_CA: Fetcher;
	CGA_BASE_URL: string;
	CGA_START_PATH: string;
	MGL_BASE_URL: string;
	MGL_START_PATH: string;
}
