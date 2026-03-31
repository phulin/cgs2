import { mkdirSync } from "node:fs";
import { resolve } from "node:path";

export interface Config {
	port: number;
	stateDir: string;
	databasePath: string;
	quickwitBaseUrl: string;
	webhookSecret: string;
	indexPrefix: string;
	defaultSearchFields: string[];
	rebuildPollMs: number;
	retainedGenerations: number;
}

function requireEnv(name: string): string {
	const value = process.env[name];
	if (!value) {
		throw new Error(`Missing required environment variable: ${name}`);
	}
	return value;
}

function parseNumber(value: string | undefined, fallback: number): number {
	if (!value) {
		return fallback;
	}
	const parsed = Number(value);
	if (!Number.isFinite(parsed)) {
		throw new Error(`Invalid numeric environment value: ${value}`);
	}
	return parsed;
}

export function loadConfig(): Config {
	const stateDir = resolve(
		process.env.FASTLAW_SEARCH_STATE_DIR ?? "./.fastlaw-search",
	);
	mkdirSync(stateDir, { recursive: true });
	mkdirSync(resolve(stateDir, "manifests"), { recursive: true });
	mkdirSync(resolve(stateDir, "blobs"), { recursive: true });

	return {
		port: parseNumber(process.env.PORT, 8788),
		stateDir,
		databasePath: resolve(
			process.env.FASTLAW_SEARCH_DB_PATH ?? `${stateDir}/search-indexer.sqlite`,
		),
		quickwitBaseUrl: requireEnv("QUICKWIT_BASE_URL").replace(/\/+$/, ""),
		webhookSecret: requireEnv("QUICKWIT_WEBHOOK_SECRET"),
		indexPrefix: process.env.QUICKWIT_INDEX_PREFIX ?? "fastlaw_latest",
		defaultSearchFields: (
			process.env.QUICKWIT_DEFAULT_SEARCH_FIELDS ??
			"title_text,search_text,breadcrumb,body_text"
		)
			.split(",")
			.map((value) => value.trim())
			.filter((value) => value.length > 0),
		rebuildPollMs: parseNumber(process.env.REBUILD_POLL_MS, 5000),
		retainedGenerations: parseNumber(process.env.RETAINED_GENERATIONS, 2),
	};
}
