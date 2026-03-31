export interface IndexingWebhookPayload {
	event: "source_export_ready";
	schema_version: number;
	source_id: string;
	source_version_id: string;
	version_date: string;
	exported_documents: number;
	manifest_url: string;
	blob_url: string;
	generated_at: string;
}

export interface ExportManifest {
	schema_version: number;
	export_id: string;
	source_id: string;
	source_version_id: string;
	version_date: string;
	doc_count: number;
	blob_path: string;
	generated_at: string;
}

export interface SearchResult {
	score: number;
	source_id: string;
	source_version_id: string;
	node_id: string;
	level_name: string;
	heading_citation: string | null;
	name: string | null;
	path: string | null;
	breadcrumb: string;
	body_text?: string | null;
}

export interface SearchRequest {
	query: string;
	top_k?: number;
	filters?: {
		source_id?: string[];
		level_name?: string[];
		region?: string[];
		doc_type?: string[];
	};
}

export type JobStatus =
	| "pending"
	| "processing"
	| "ready"
	| "failed"
	| "duplicate";

export type GenerationStatus =
	| "building"
	| "active"
	| "superseded"
	| "failed";
