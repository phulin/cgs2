import { createWriteStream } from "node:fs";
import { mkdir, rm } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { randomUUID } from "node:crypto";
import type { Config } from "./config.js";
import { SearchIndexerDb, type IngestJobRow } from "./db.js";
import { buildIndexId, QuickwitClient } from "./quickwit.js";
import type {
	ExportManifest,
	IndexingWebhookPayload,
	JobStatus,
} from "./types.js";

function slug(value: string): string {
	return value.replace(/[^a-zA-Z0-9._-]+/g, "_");
}

function nowStamp(): string {
	return new Date().toISOString().replace(/[:.]/g, "-");
}

export class SearchIndexerService {
	private timer: NodeJS.Timeout | null = null;
	private runningTick = false;

	constructor(
		private readonly config: Config,
		private readonly db: SearchIndexerDb,
		private readonly quickwit: QuickwitClient,
	) {}

	start(): void {
		this.timer = setInterval(() => {
			void this.tick().catch((error) => {
				console.error("[search-indexer] background tick failed", error);
			});
		}, this.config.rebuildPollMs);
		void this.tick().catch((error) => {
			console.error("[search-indexer] initial tick failed", error);
		});
	}

	stop(): void {
		if (this.timer) {
			clearInterval(this.timer);
			this.timer = null;
		}
	}

	async enqueueWebhook(payload: IndexingWebhookPayload): Promise<{
		jobId: string;
		status: JobStatus;
	}> {
		const manifest = await this.fetchManifest(payload.manifest_url);
		if (
			manifest.source_id !== payload.source_id ||
			manifest.source_version_id !== payload.source_version_id
		) {
			throw new Error("Manifest/source mismatch.");
		}

		const existing = this.db.getJobBySourceExport(
			payload.source_id,
			payload.source_version_id,
			manifest.export_id,
		);
		if (existing) {
			return {
				jobId: existing.id,
				status: existing.status,
			};
		}

		const jobId = randomUUID();
		const { row } = this.db.addJob({
			id: jobId,
			sourceId: payload.source_id,
			sourceVersionId: payload.source_version_id,
			exportId: manifest.export_id,
			payloadJson: JSON.stringify(payload),
		});
		this.db.addJobEvent(jobId, "enqueued", "Webhook accepted.");
		void this.tick().catch((error) => {
			console.error("[search-indexer] enqueue tick failed", error);
		});
		return {
			jobId: row.id,
			status: row.status,
		};
	}

	getJob(jobId: string) {
		return this.db.getJob(jobId);
	}

	getState() {
		return {
			generationState: this.db.getState(),
			generations: this.db.listGenerations(),
			latestExports: this.db.listLatestExports(),
		};
	}

	requestRebuild(): void {
		this.db.requestRebuild();
		void this.tick().catch((error) => {
			console.error("[search-indexer] rebuild tick failed", error);
		});
	}

	private async tick(): Promise<void> {
		if (this.runningTick) {
			return;
		}
		this.runningTick = true;
		try {
			while (true) {
				const pending = this.db.getNextPendingJob();
				if (!pending) {
					break;
				}
				await this.processJob(pending);
			}

			if (this.db.tryStartRebuild()) {
				try {
					await this.rebuild();
				} finally {
					this.db.finishRebuild();
				}
			}
		} finally {
			this.runningTick = false;
		}
	}

	private async processJob(job: IngestJobRow): Promise<void> {
		this.db.markJobProcessing(job.id);
		this.db.addJobEvent(job.id, "processing", "Starting artifact download.");

		try {
			const payload = JSON.parse(job.payload_json) as IndexingWebhookPayload;
			const manifest = await this.fetchManifest(payload.manifest_url);
			const manifestPath = resolve(
				this.config.stateDir,
				"manifests",
				`${slug(job.source_id)}-${slug(job.source_version_id)}-${slug(manifest.export_id)}.json`,
			);
			const blobPath = resolve(
				this.config.stateDir,
				"blobs",
				`${slug(job.source_id)}-${slug(job.source_version_id)}-${slug(manifest.export_id)}.ndjson`,
			);

			await mkdir(dirname(manifestPath), { recursive: true });
			await mkdir(dirname(blobPath), { recursive: true });
			await this.downloadToFile(payload.manifest_url, manifestPath);
			await this.downloadToFile(payload.blob_url, blobPath);

			this.db.upsertLatestExport({
				sourceId: job.source_id,
				sourceVersionId: job.source_version_id,
				exportId: manifest.export_id,
				versionDate: manifest.version_date,
				docCount: manifest.doc_count,
				manifestPath,
				blobPath,
				generatedAt: manifest.generated_at,
			});
			this.db.markJobReady({
				id: job.id,
				manifestPath,
				blobPath,
			});
			this.db.addJobEvent(job.id, "ready", "Artifacts downloaded.");
			this.db.requestRebuild();
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			this.db.markJobFailed(job.id, message);
			this.db.addJobEvent(job.id, "failed", message);
		}
	}

	private async rebuild(): Promise<void> {
		const exports = this.db.listLatestExports();
		if (exports.length === 0) {
			return;
		}

		const generationId = `latest-${nowStamp()}`;
		const indexId = buildIndexId(this.config, generationId);
		this.db.insertGeneration({
			generationId,
			indexId,
			status: "building",
		});

		try {
			await this.quickwit.createIndex(indexId);
			let totalDocs = 0;
			for (const exportRow of exports) {
				totalDocs += await this.quickwit.ingestFile(indexId, exportRow.blob_path);
			}
			await this.quickwit.smokeTest(indexId);
			this.db.updateGeneration({
				generationId,
				status: "building",
				docCount: totalDocs,
			});
			this.db.activateGeneration(generationId);
			await this.cleanupOldGenerations();
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			this.db.updateGeneration({
				generationId,
				status: "failed",
				error: message,
			});
			await this.quickwit.deleteIndex(indexId).catch((deleteError) => {
				console.error(
					`[search-indexer] failed to delete failed index ${indexId}`,
					deleteError,
				);
			});
			throw error;
		}
	}

	private async cleanupOldGenerations(): Promise<void> {
		const oldGenerations = this.db.listOldGenerationsToDelete(
			this.config.retainedGenerations,
		);
		for (const generation of oldGenerations) {
			try {
				await this.quickwit.deleteIndex(generation.index_id);
				this.db.deleteGeneration(generation.generation_id);
			} catch (error) {
				console.error(
					`[search-indexer] failed to delete old index ${generation.index_id}`,
					error,
				);
			}
		}
	}

	private async fetchManifest(url: string): Promise<ExportManifest> {
		const response = await fetch(url);
		if (!response.ok) {
			throw new Error(
				`Failed to download manifest: ${response.status} ${await response.text()}`,
			);
		}
		return (await response.json()) as ExportManifest;
	}

	private async downloadToFile(url: string, filePath: string): Promise<void> {
		const response = await fetch(url);
		if (!response.ok || !response.body) {
			throw new Error(
				`Failed to download artifact: ${response.status} ${await response.text()}`,
			);
		}
		const file = createWriteStream(filePath, { flags: "w" });
		try {
			await pipeline(response.body, file);
		} catch (error) {
			await rm(filePath, { force: true }).catch(() => undefined);
			throw error;
		}
	}
}
