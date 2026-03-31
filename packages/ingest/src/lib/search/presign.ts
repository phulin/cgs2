interface PresignGetUrlArgs {
	accountId: string;
	accessKeyId: string;
	secretAccessKey: string;
	bucket: string;
	key: string;
	expiresInSeconds: number;
}

function toHex(buffer: ArrayBuffer | Uint8Array): string {
	const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
	return [...bytes]
		.map((byte) => byte.toString(16).padStart(2, "0"))
		.join("");
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
	return bytes.buffer.slice(
		bytes.byteOffset,
		bytes.byteOffset + bytes.byteLength,
	) as ArrayBuffer;
}

function encodeRfc3986(value: string): string {
	return encodeURIComponent(value).replace(
		/[!'()*]/g,
		(character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
	);
}

function encodePath(key: string): string {
	return key
		.split("/")
		.map((segment) => encodeRfc3986(segment))
		.join("/");
}

function formatAmzDate(date: Date): { shortDate: string; longDate: string } {
	const iso = date.toISOString().replace(/[:-]|\.\d{3}/g, "");
	return {
		shortDate: iso.slice(0, 8),
		longDate: iso,
	};
}

async function sha256Hex(value: string): Promise<string> {
	return toHex(
		await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)),
	);
}

async function hmacSha256(
	keyBytes: Uint8Array,
	value: string,
): Promise<Uint8Array> {
	const cryptoKey = await crypto.subtle.importKey(
		"raw",
		toArrayBuffer(keyBytes),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);
	return new Uint8Array(
		await crypto.subtle.sign(
			"HMAC",
			cryptoKey,
			new TextEncoder().encode(value),
		),
	);
}

async function deriveSigningKey(args: {
	secretAccessKey: string;
	shortDate: string;
	region: string;
	service: string;
}): Promise<Uint8Array> {
	const dateKey = await hmacSha256(
		new TextEncoder().encode(`AWS4${args.secretAccessKey}`),
		args.shortDate,
	);
	const regionKey = await hmacSha256(dateKey, args.region);
	const serviceKey = await hmacSha256(regionKey, args.service);
	return await hmacSha256(serviceKey, "aws4_request");
}

export async function presignR2GetUrl(
	args: PresignGetUrlArgs,
): Promise<string> {
	const expiresInSeconds = Math.max(1, Math.min(604800, args.expiresInSeconds));
	const method = "GET";
	const region = "auto";
	const service = "s3";
	const host = `${args.accountId}.r2.cloudflarestorage.com`;
	const canonicalUri = `/${encodeRfc3986(args.bucket)}/${encodePath(args.key)}`;
	const { shortDate, longDate } = formatAmzDate(new Date());
	const credentialScope = `${shortDate}/${region}/${service}/aws4_request`;

	const query = new URLSearchParams({
		"X-Amz-Algorithm": "AWS4-HMAC-SHA256",
		"X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
		"X-Amz-Credential": `${args.accessKeyId}/${credentialScope}`,
		"X-Amz-Date": longDate,
		"X-Amz-Expires": String(expiresInSeconds),
		"X-Amz-SignedHeaders": "host",
		"x-id": "GetObject",
	});
	const canonicalQueryString = [...query.entries()]
		.sort(([left], [right]) => left.localeCompare(right))
		.map(([key, value]) => `${encodeRfc3986(key)}=${encodeRfc3986(value)}`)
		.join("&");

	const canonicalRequest = [
		method,
		canonicalUri,
		canonicalQueryString,
		`host:${host}\n`,
		"host",
		"UNSIGNED-PAYLOAD",
	].join("\n");

	const stringToSign = [
		"AWS4-HMAC-SHA256",
		longDate,
		credentialScope,
		await sha256Hex(canonicalRequest),
	].join("\n");

	const signingKey = await deriveSigningKey({
		secretAccessKey: args.secretAccessKey,
		shortDate,
		region,
		service,
	});
	const signature = toHex(await hmacSha256(signingKey, stringToSign));
	query.set("X-Amz-Signature", signature);

	return `https://${host}${canonicalUri}?${query.toString()}`;
}
