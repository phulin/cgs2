import { timingSafeEqual } from "node:crypto";

function toHex(buffer: ArrayBuffer): string {
	return [...new Uint8Array(buffer)]
		.map((byte) => byte.toString(16).padStart(2, "0"))
		.join("");
}

async function hmacHex(secret: string, payload: string): Promise<string> {
	const key = await crypto.subtle.importKey(
		"raw",
		new TextEncoder().encode(secret),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);
	const signature = await crypto.subtle.sign(
		"HMAC",
		key,
		new TextEncoder().encode(payload),
	);
	return toHex(signature);
}

export async function createWebhookSignature(args: {
	secret: string;
	timestamp: string;
	body: string;
}): Promise<string> {
	return await hmacHex(args.secret, `${args.timestamp}\n${args.body}`);
}

export async function verifyWebhookSignature(args: {
	secret: string;
	timestamp: string | null;
	signature: string | null;
	body: string;
	maxSkewMs?: number;
}): Promise<void> {
	if (!args.timestamp || !args.signature) {
		throw new Error("Missing webhook signature headers.");
	}

	const timestampMs = Date.parse(args.timestamp);
	if (Number.isNaN(timestampMs)) {
		throw new Error("Invalid webhook timestamp.");
	}

	const maxSkewMs = args.maxSkewMs ?? 5 * 60 * 1000;
	if (Math.abs(Date.now() - timestampMs) > maxSkewMs) {
		throw new Error("Webhook timestamp outside allowed skew.");
	}

	const expected = await createWebhookSignature({
		secret: args.secret,
		timestamp: args.timestamp,
		body: args.body,
	});

	const actualBuffer = Buffer.from(args.signature, "hex");
	const expectedBuffer = Buffer.from(expected, "hex");
	if (
		actualBuffer.length !== expectedBuffer.length ||
		!timingSafeEqual(actualBuffer, expectedBuffer)
	) {
		throw new Error("Invalid webhook signature.");
	}
}
