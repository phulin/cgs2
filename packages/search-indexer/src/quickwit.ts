import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import type { Config } from "./config.js";
import type { SearchRequest, SearchResult } from "./types.js";

const QUICKWIT_INDEX_CONFIG_VERSION = "0.8";
const INGEST_BATCH_SIZE = 500;

function sanitizeIndexSegment(value: string): string {
	return value.toLowerCase().replace(/[^a-z0-9_]+/g, "_");
}

function escapeQuickwitTerm(value: string): string {
	return value.replace(/(["\\])/g, "\\$1");
}

export function buildIndexId(config: Config, generationId: string): string {
	return `${sanitizeIndexSegment(config.indexPrefix)}_${sanitizeIndexSegment(generationId)}`;
}

function buildIndexConfigYaml(config: Config, indexId: string): string {
	return `
version: ${QUICKWIT_INDEX_CONFIG_VERSION}
index_id: ${indexId}
doc_mapping:
  mode: lenient
  field_mappings:
    - name: doc_id
      type: text
      tokenizer: raw
      stored: true
    - name: source_id
      type: text
      tokenizer: raw
      stored: true
      fast: true
    - name: source_name
      type: text
      stored: true
    - name: jurisdiction
      type: text
      tokenizer: raw
      stored: true
      fast: true
    - name: region
      type: text
      tokenizer: raw
      stored: true
      fast: true
    - name: doc_type
      type: text
      tokenizer: raw
      stored: true
      fast: true
    - name: source_version_id
      type: text
      tokenizer: raw
      stored: true
      fast: true
    - name: version_date
      type: datetime
      stored: true
      fast: true
    - name: node_id
      type: text
      tokenizer: raw
      stored: true
    - name: parent_id
      type: text
      tokenizer: raw
      stored: true
    - name: level_name
      type: text
      tokenizer: raw
      stored: true
      fast: true
    - name: level_index
      type: u64
      stored: true
      fast: true
    - name: sort_order
      type: u64
      stored: true
      fast: true
    - name: path
      type: text
      tokenizer: raw
      stored: true
    - name: readable_id
      type: text
      tokenizer: raw
      stored: true
    - name: heading_citation
      type: text
      stored: true
    - name: name
      type: text
      stored: true
    - name: source_url
      type: text
      tokenizer: raw
      stored: true
    - name: accessed_at
      type: text
      tokenizer: raw
      stored: true
    - name: blob_hash
      type: text
      tokenizer: raw
      stored: true
    - name: has_body
      type: bool
      stored: true
      fast: true
    - name: breadcrumb
      type: text
      stored: true
    - name: title_text
      type: text
      stored: true
    - name: body_text
      type: text
      stored: true
    - name: search_text
      type: text
      stored: true
search_settings:
  default_search_fields: [${config.defaultSearchFields
		.map((field) => `"${field}"`)
		.join(", ")}]
indexing_settings: {}
`.trim();
}

async function parseError(response: Response): Promise<string> {
	const text = await response.text();
	return text || `status=${response.status}`;
}

export class QuickwitClient {
	constructor(private readonly config: Config) {}

	private async request(
		path: string,
		init?: RequestInit,
	): Promise<Response> {
		return await fetch(`${this.config.quickwitBaseUrl}${path}`, init);
	}

	async createIndex(indexId: string): Promise<void> {
		const response = await this.request("/api/v1/indexes", {
			method: "POST",
			headers: {
				"Content-Type": "application/yaml",
			},
			body: buildIndexConfigYaml(this.config, indexId),
		});
		if (!response.ok) {
			throw new Error(`Quickwit createIndex failed: ${await parseError(response)}`);
		}
	}

	async deleteIndex(indexId: string): Promise<void> {
		const response = await this.request(`/api/v1/indexes/${indexId}`, {
			method: "DELETE",
		});
		if (!response.ok && response.status !== 404) {
			throw new Error(`Quickwit deleteIndex failed: ${await parseError(response)}`);
		}
	}

	async ingestFile(indexId: string, filePath: string): Promise<number> {
		const reader = createInterface({
			input: createReadStream(filePath, { encoding: "utf8" }),
			crlfDelay: Infinity,
		});
		let count = 0;
		let batch: string[] = [];

		for await (const line of reader) {
			if (!line.trim()) {
				continue;
			}
			batch.push(line);
			count += 1;
			if (batch.length >= INGEST_BATCH_SIZE) {
				await this.ingestBatch(indexId, batch);
				batch = [];
			}
		}

		if (batch.length > 0) {
			await this.ingestBatch(indexId, batch);
		}

		return count;
	}

	private async ingestBatch(indexId: string, lines: string[]): Promise<void> {
		const response = await this.request(
			`/api/v1/${indexId}/ingest?commit=force`,
			{
				method: "POST",
				headers: {
					"Content-Type": "application/x-ndjson",
				},
				body: `${lines.join("\n")}\n`,
			},
		);
		if (!response.ok) {
			throw new Error(`Quickwit ingest failed: ${await parseError(response)}`);
		}
	}

	async smokeTest(indexId: string): Promise<void> {
		const response = await this.request(
			`/api/v1/${indexId}/search?query=*`,
			{
				method: "GET",
			},
		);
		if (!response.ok) {
			throw new Error(`Quickwit smoke test failed: ${await parseError(response)}`);
		}
	}

	async search(indexId: string, request: SearchRequest): Promise<{
		num_hits: number;
		hits: SearchResult[];
	}> {
		const topK = request.top_k ?? 20;
		const query = this.buildQuery(request);
		const response = await this.request(`/api/v1/${indexId}/search`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
			},
			body: JSON.stringify({
				query,
				max_hits: topK,
			}),
		});
		if (!response.ok) {
			throw new Error(`Quickwit search failed: ${await parseError(response)}`);
		}

		const body = (await response.json()) as {
			num_hits?: number;
			hits?: Array<{ _source?: SearchResult }>;
		};
		return {
			num_hits: body.num_hits ?? 0,
			hits: (body.hits ?? []).map((hit) => hit._source ?? ({} as SearchResult)),
		};
	}

	private buildQuery(request: SearchRequest): string {
		const parts = [`(${request.query})`];
		if (request.filters?.source_id?.length) {
			parts.push(
				`(${request.filters.source_id
					.map((value) => `source_id:"${escapeQuickwitTerm(value)}"`)
					.join(" OR ")})`,
			);
		}
		if (request.filters?.level_name?.length) {
			parts.push(
				`(${request.filters.level_name
					.map((value) => `level_name:"${escapeQuickwitTerm(value)}"`)
					.join(" OR ")})`,
			);
		}
		if (request.filters?.region?.length) {
			parts.push(
				`(${request.filters.region
					.map((value) => `region:"${escapeQuickwitTerm(value)}"`)
					.join(" OR ")})`,
			);
		}
		if (request.filters?.doc_type?.length) {
			parts.push(
				`(${request.filters.doc_type
					.map((value) => `doc_type:"${escapeQuickwitTerm(value)}"`)
					.join(" OR ")})`,
			);
		}
		return parts.join(" AND ");
	}
}
