import { createServer } from "node:http";
import { loadConfig } from "./config.js";
import { SearchIndexerDb } from "./db.js";
import { QuickwitClient } from "./quickwit.js";
import { SearchIndexerService } from "./service.js";
import { verifyWebhookSignature } from "./signature.js";
import type { IndexingWebhookPayload, SearchRequest } from "./types.js";

async function readBody(request: AsyncIterable<Uint8Array>): Promise<string> {
	const chunks: Uint8Array[] = [];
	for await (const chunk of request) {
		chunks.push(chunk);
	}
	return Buffer.concat(chunks).toString("utf8");
}

function jsonResponse(
	body: unknown,
	status = 200,
): { status: number; headers: Record<string, string>; body: string } {
	return {
		status,
		headers: {
			"Content-Type": "application/json; charset=utf-8",
		},
		body: JSON.stringify(body),
	};
}

const config = loadConfig();
const db = new SearchIndexerDb(config.databasePath);
const quickwit = new QuickwitClient(config);
const service = new SearchIndexerService(config, db, quickwit);
service.start();

const server = createServer(async (request, response) => {
	try {
		const url = new URL(request.url ?? "/", `http://${request.headers.host}`);
		const method = request.method ?? "GET";

		if (method === "GET" && url.pathname === "/healthz") {
			const result = jsonResponse({
				ok: true,
				state: service.getState(),
			});
			response.writeHead(result.status, result.headers);
			response.end(result.body);
			return;
		}

		if (method === "POST" && url.pathname === "/indexing-jobs") {
			const body = await readBody(request);
			try {
				await verifyWebhookSignature({
					secret: config.webhookSecret,
					timestamp:
						typeof request.headers["x-fastlaw-timestamp"] === "string"
							? request.headers["x-fastlaw-timestamp"]
							: null,
					signature:
						typeof request.headers["x-fastlaw-signature"] === "string"
							? request.headers["x-fastlaw-signature"]
							: null,
					body,
				});
				const payload = JSON.parse(body) as IndexingWebhookPayload;
				const result = await service.enqueueWebhook(payload);
				const output = jsonResponse(result, 202);
				response.writeHead(output.status, output.headers);
				response.end(output.body);
				return;
			} catch (error) {
				const output = jsonResponse(
					{ error: error instanceof Error ? error.message : String(error) },
					401,
				);
				response.writeHead(output.status, output.headers);
				response.end(output.body);
				return;
			}
		}

		if (method === "GET" && url.pathname.startsWith("/indexing-jobs/")) {
			const jobId = url.pathname.slice("/indexing-jobs/".length);
			const job = service.getJob(jobId);
			const output = job
				? jsonResponse({ job })
				: jsonResponse({ error: "Job not found" }, 404);
			response.writeHead(output.status, output.headers);
			response.end(output.body);
			return;
		}

		if (method === "POST" && url.pathname === "/rebuild") {
			service.requestRebuild();
			const output = jsonResponse({ queued: true }, 202);
			response.writeHead(output.status, output.headers);
			response.end(output.body);
			return;
		}

		if (method === "POST" && url.pathname === "/search") {
			const body = await readBody(request);
			const searchRequest = JSON.parse(body || "{}") as SearchRequest;
			if (!searchRequest.query?.trim()) {
				const output = jsonResponse({ error: "Missing query" }, 400);
				response.writeHead(output.status, output.headers);
				response.end(output.body);
				return;
			}

			const state = db.getState();
			if (!state.active_generation_id) {
				const output = jsonResponse({ error: "No active generation" }, 503);
				response.writeHead(output.status, output.headers);
				response.end(output.body);
				return;
			}

			const generation = db
				.listGenerations()
				.find((item) => item.generation_id === state.active_generation_id);
			if (!generation) {
				const output = jsonResponse(
					{ error: "Active generation metadata missing" },
					500,
				);
				response.writeHead(output.status, output.headers);
				response.end(output.body);
				return;
			}

			try {
				const result = await quickwit.search(generation.index_id, searchRequest);
				const output = jsonResponse({
					query: searchRequest.query,
					generation: generation.generation_id,
					num_hits: result.num_hits,
					results: result.hits,
				});
				response.writeHead(output.status, output.headers);
				response.end(output.body);
				return;
			} catch (error) {
				const output = jsonResponse(
					{ error: error instanceof Error ? error.message : String(error) },
					500,
				);
				response.writeHead(output.status, output.headers);
				response.end(output.body);
				return;
			}
		}

		const output = jsonResponse({ error: "Not found" }, 404);
		response.writeHead(output.status, output.headers);
		response.end(output.body);
	} catch (error) {
		const output = jsonResponse(
			{ error: error instanceof Error ? error.message : String(error) },
			500,
		);
		response.writeHead(output.status, output.headers);
		response.end(output.body);
	}
});

server.listen(config.port, () => {
	console.log(`[search-indexer] listening on http://localhost:${config.port}`);
});
