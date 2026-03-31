import { DatabaseSync } from "node:sqlite";
import type { GenerationStatus, JobStatus } from "./types.js";

export interface LatestExportRow {
	source_id: string;
	source_version_id: string;
	export_id: string;
	version_date: string;
	doc_count: number;
	manifest_path: string;
	blob_path: string;
	generated_at: string;
	updated_at: string;
}

export interface IngestJobRow {
	id: string;
	source_id: string;
	source_version_id: string;
	export_id: string;
	status: JobStatus;
	payload_json: string;
	manifest_path: string | null;
	blob_path: string | null;
	error: string | null;
	created_at: string;
	updated_at: string;
	started_at: string | null;
	completed_at: string | null;
}

export interface GenerationRow {
	generation_id: string;
	index_id: string;
	status: GenerationStatus;
	doc_count: number;
	error: string | null;
	created_at: string;
	activated_at: string | null;
}

function now(): string {
	return new Date().toISOString();
}

export class SearchIndexerDb {
	private db: DatabaseSync;

	constructor(path: string) {
		this.db = new DatabaseSync(path);
		this.db.exec("PRAGMA journal_mode = WAL;");
		this.db.exec("PRAGMA foreign_keys = ON;");
		this.migrate();
	}

	private migrate(): void {
		this.db.exec(`
			CREATE TABLE IF NOT EXISTS ingest_jobs (
				id TEXT PRIMARY KEY,
				source_id TEXT NOT NULL,
				source_version_id TEXT NOT NULL,
				export_id TEXT NOT NULL,
				status TEXT NOT NULL,
				payload_json TEXT NOT NULL,
				manifest_path TEXT,
				blob_path TEXT,
				error TEXT,
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				started_at TEXT,
				completed_at TEXT
			);

			CREATE UNIQUE INDEX IF NOT EXISTS ingest_jobs_source_export
			ON ingest_jobs(source_id, source_version_id, export_id);

			CREATE TABLE IF NOT EXISTS latest_exports (
				source_id TEXT PRIMARY KEY,
				source_version_id TEXT NOT NULL,
				export_id TEXT NOT NULL,
				version_date TEXT NOT NULL,
				doc_count INTEGER NOT NULL,
				manifest_path TEXT NOT NULL,
				blob_path TEXT NOT NULL,
				generated_at TEXT NOT NULL,
				updated_at TEXT NOT NULL
			);

			CREATE TABLE IF NOT EXISTS generations (
				generation_id TEXT PRIMARY KEY,
				index_id TEXT NOT NULL UNIQUE,
				status TEXT NOT NULL,
				doc_count INTEGER NOT NULL DEFAULT 0,
				error TEXT,
				created_at TEXT NOT NULL,
				activated_at TEXT
			);

			CREATE TABLE IF NOT EXISTS generation_state (
				id INTEGER PRIMARY KEY CHECK (id = 1),
				active_generation_id TEXT,
				previous_generation_id TEXT,
				rebuild_requested INTEGER NOT NULL DEFAULT 0,
				rebuild_in_progress INTEGER NOT NULL DEFAULT 0,
				updated_at TEXT NOT NULL
			);

			INSERT INTO generation_state (id, updated_at)
			VALUES (1, CURRENT_TIMESTAMP)
			ON CONFLICT (id) DO NOTHING;

			CREATE TABLE IF NOT EXISTS job_events (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				job_id TEXT NOT NULL,
				event_type TEXT NOT NULL,
				message TEXT NOT NULL,
				created_at TEXT NOT NULL
			);
		`);
	}

	addJob(args: {
		id: string;
		sourceId: string;
		sourceVersionId: string;
		exportId: string;
		payloadJson: string;
	}): { inserted: boolean; row: IngestJobRow } {
		const createdAt = now();
		this.db
			.prepare(
				`INSERT OR IGNORE INTO ingest_jobs (
					id, source_id, source_version_id, export_id, status, payload_json,
					created_at, updated_at
				) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)`,
			)
			.run(
				args.id,
				args.sourceId,
				args.sourceVersionId,
				args.exportId,
				args.payloadJson,
				createdAt,
				createdAt,
			);
		const row = this.getJobBySourceExport(
			args.sourceId,
			args.sourceVersionId,
			args.exportId,
		);
		if (!row) {
			throw new Error("Failed to load persisted ingest job.");
		}
		return { inserted: row.id === args.id, row };
	}

	getJob(id: string): IngestJobRow | null {
		return (
			this.db
				.prepare(`SELECT * FROM ingest_jobs WHERE id = ?`)
				.get(id) as IngestJobRow | undefined
		) ?? null;
	}

	getJobBySourceExport(
		sourceId: string,
		sourceVersionId: string,
		exportId: string,
	): IngestJobRow | null {
		return (
			this.db
				.prepare(
					`SELECT * FROM ingest_jobs
					WHERE source_id = ? AND source_version_id = ? AND export_id = ?`,
				)
				.get(sourceId, sourceVersionId, exportId) as IngestJobRow | undefined
		) ?? null;
	}

	getNextPendingJob(): IngestJobRow | null {
		return (
			this.db
				.prepare(
					`SELECT * FROM ingest_jobs
					WHERE status = 'pending'
					ORDER BY created_at ASC
					LIMIT 1`,
				)
				.get() as IngestJobRow | undefined
		) ?? null;
	}

	markJobProcessing(id: string): void {
		const timestamp = now();
		this.db
			.prepare(
				`UPDATE ingest_jobs
				SET status = 'processing', started_at = ?, updated_at = ?
				WHERE id = ? AND status = 'pending'`,
			)
			.run(timestamp, timestamp, id);
	}

	markJobReady(args: {
		id: string;
		manifestPath: string;
		blobPath: string;
	}): void {
		const timestamp = now();
		this.db
			.prepare(
				`UPDATE ingest_jobs
				SET status = 'ready',
					manifest_path = ?,
					blob_path = ?,
					error = NULL,
					completed_at = ?,
					updated_at = ?
				WHERE id = ?`,
			)
			.run(args.manifestPath, args.blobPath, timestamp, timestamp, args.id);
	}

	markJobDuplicate(id: string): void {
		const timestamp = now();
		this.db
			.prepare(
				`UPDATE ingest_jobs
				SET status = 'duplicate',
					completed_at = ?,
					updated_at = ?
				WHERE id = ?`,
			)
			.run(timestamp, timestamp, id);
	}

	markJobFailed(id: string, error: string): void {
		const timestamp = now();
		this.db
			.prepare(
				`UPDATE ingest_jobs
				SET status = 'failed',
					error = ?,
					completed_at = ?,
					updated_at = ?
				WHERE id = ?`,
			)
			.run(error, timestamp, timestamp, id);
	}

	addJobEvent(jobId: string, eventType: string, message: string): void {
		this.db
			.prepare(
				`INSERT INTO job_events (job_id, event_type, message, created_at)
				VALUES (?, ?, ?, ?)`,
			)
			.run(jobId, eventType, message, now());
	}

	upsertLatestExport(args: {
		sourceId: string;
		sourceVersionId: string;
		exportId: string;
		versionDate: string;
		docCount: number;
		manifestPath: string;
		blobPath: string;
		generatedAt: string;
	}): void {
		this.db
			.prepare(
				`INSERT INTO latest_exports (
					source_id, source_version_id, export_id, version_date, doc_count,
					manifest_path, blob_path, generated_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(source_id) DO UPDATE SET
					source_version_id = excluded.source_version_id,
					export_id = excluded.export_id,
					version_date = excluded.version_date,
					doc_count = excluded.doc_count,
					manifest_path = excluded.manifest_path,
					blob_path = excluded.blob_path,
					generated_at = excluded.generated_at,
					updated_at = excluded.updated_at`,
			)
			.run(
				args.sourceId,
				args.sourceVersionId,
				args.exportId,
				args.versionDate,
				args.docCount,
				args.manifestPath,
				args.blobPath,
				args.generatedAt,
				now(),
			);
	}

	listLatestExports(): LatestExportRow[] {
		return this.db
			.prepare(`SELECT * FROM latest_exports ORDER BY source_id ASC`)
			.all() as unknown as LatestExportRow[];
	}

	requestRebuild(): void {
		this.db
			.prepare(
				`UPDATE generation_state
				SET rebuild_requested = 1, updated_at = ?
				WHERE id = 1`,
			)
			.run(now());
	}

	getState(): {
		active_generation_id: string | null;
		previous_generation_id: string | null;
		rebuild_requested: number;
		rebuild_in_progress: number;
	} {
		const state = this.db
			.prepare(
				`SELECT active_generation_id, previous_generation_id, rebuild_requested, rebuild_in_progress
				FROM generation_state
				WHERE id = 1`,
			)
			.get() as
			| {
					active_generation_id: string | null;
					previous_generation_id: string | null;
					rebuild_requested: number;
					rebuild_in_progress: number;
			  }
			| undefined;
		if (!state) {
			throw new Error("Missing generation_state row.");
		}
		return state;
	}

	tryStartRebuild(): boolean {
		const result = this.db
			.prepare(
				`UPDATE generation_state
				SET rebuild_requested = 0,
					rebuild_in_progress = 1,
					updated_at = ?
				WHERE id = 1
					AND rebuild_requested = 1
					AND rebuild_in_progress = 0`,
			)
			.run(now());
		return result.changes > 0;
	}

	finishRebuild(): void {
		this.db
			.prepare(
				`UPDATE generation_state
				SET rebuild_in_progress = 0, updated_at = ?
				WHERE id = 1`,
			)
			.run(now());
	}

	insertGeneration(args: {
		generationId: string;
		indexId: string;
		status: GenerationStatus;
		docCount?: number;
		error?: string | null;
	}): void {
		this.db
			.prepare(
				`INSERT INTO generations (
					generation_id, index_id, status, doc_count, error, created_at
				) VALUES (?, ?, ?, ?, ?, ?)`,
			)
			.run(
				args.generationId,
				args.indexId,
				args.status,
				args.docCount ?? 0,
				args.error ?? null,
				now(),
			);
	}

	updateGeneration(args: {
		generationId: string;
		status: GenerationStatus;
		docCount?: number;
		error?: string | null;
		activatedAt?: string | null;
	}): void {
		this.db
			.prepare(
				`UPDATE generations
				SET status = ?,
					doc_count = COALESCE(?, doc_count),
					error = ?,
					activated_at = COALESCE(?, activated_at)
				WHERE generation_id = ?`,
			)
			.run(
				args.status,
				args.docCount ?? null,
				args.error ?? null,
				args.activatedAt ?? null,
				args.generationId,
			);
	}

	activateGeneration(generationId: string): void {
		const current = this.getState();
		const activatedAt = now();
		this.db.exec("BEGIN");
		try {
			if (current.active_generation_id) {
				this.db
					.prepare(
						`UPDATE generations
						SET status = 'superseded'
						WHERE generation_id = ?`,
					)
					.run(current.active_generation_id);
			}
			this.db
				.prepare(
					`UPDATE generations
					SET status = 'active', activated_at = ?
					WHERE generation_id = ?`,
				)
				.run(activatedAt, generationId);
			this.db
				.prepare(
					`UPDATE generation_state
					SET active_generation_id = ?,
						previous_generation_id = ?,
						updated_at = ?
					WHERE id = 1`,
				)
				.run(generationId, current.active_generation_id, activatedAt);
			this.db.exec("COMMIT");
		} catch (error) {
			this.db.exec("ROLLBACK");
			throw error;
		}
	}

	listGenerations(): GenerationRow[] {
		return this.db
			.prepare(`SELECT * FROM generations ORDER BY created_at DESC`)
			.all() as unknown as GenerationRow[];
	}

	listOldGenerationsToDelete(retained: number): GenerationRow[] {
		const keep = Math.max(1, retained);
		return this.db
			.prepare(
				`SELECT * FROM generations
				WHERE generation_id NOT IN (
					SELECT generation_id FROM generations
					ORDER BY created_at DESC
					LIMIT ?
				)
				ORDER BY created_at ASC`,
			)
			.all(keep) as unknown as GenerationRow[];
	}

	deleteGeneration(generationId: string): void {
		this.db
			.prepare(`DELETE FROM generations WHERE generation_id = ?`)
			.run(generationId);
	}
}
