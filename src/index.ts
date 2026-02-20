import { Env } from "./external";

type DirectMatch = {
	url: string;
	fileType: string;
	jsonPath: string;
};

const UPSTREAM_HOST = "https://jbangdev.github.io";
const UPSTREAM_METADATA_PREFIX = "/jdkdb-data/metadata/";

function buildUpstreamMetadataUrl(inbound: URL): URL | null {
	// Worker is mounted under some prefix ending in /metadata/* (e.g. /java/metadata/* or /metadata/*).
	// We map whatever comes after the first "/metadata/" segment to the upstream repo's /metadata/.
	const marker = "/metadata/";
	const idx = inbound.pathname.indexOf(marker);
	if (idx === -1) return null;

	const rest = inbound.pathname.slice(idx + marker.length); // e.g. "ga/linux/x86_64/..."
	const upstream = new URL(UPSTREAM_HOST);
	upstream.pathname = `${UPSTREAM_METADATA_PREFIX}${rest}`;
	upstream.search = inbound.search;
	upstream.hash = inbound.hash;
	return upstream;
}

function isDirectJsonRequest(url: URL): boolean {
	return url.searchParams.has("direct") && url.pathname.toLowerCase().endsWith(".json");
}

function findFirstDirectMatch(value: unknown): DirectMatch | null {
	const targetTypes = new Set(["tar.gz", "zip"]);
	const seen = new Set<unknown>();

	const walk = (node: unknown, path: string): DirectMatch | null => {
		if (node === null || typeof node !== "object") return null;
		if (seen.has(node)) return null;
		seen.add(node);

		if (Array.isArray(node)) {
			for (let i = 0; i < node.length; i++) {
				const hit = walk(node[i], `${path}[${i}]`);
				if (hit) return hit;
			}
			return null;
		}

		const obj = node as Record<string, unknown>;
		const fileType = typeof obj.file_type === "string" ? obj.file_type.toLowerCase() : null;
		const url = typeof obj.url === "string" ? obj.url : null;

		if (fileType && url && targetTypes.has(fileType)) {
			return { url, fileType, jsonPath: path };
		}

		for (const [k, v] of Object.entries(obj)) {
			const hit = walk(v, path ? `${path}.${k}` : k);
			if (hit) return hit;
		}
		return null;
	};

	return walk(value, "");
}

function notFoundWithContext(message: string, context: Record<string, unknown>): Response {
	const body =
		`404 Not Found\n\n${message}\n\n` +
		`Context:\n` +
		`${JSON.stringify(context, null, 2)}\n`;

	return new Response(body, {
		status: 404,
		headers: { "content-type": "text/plain; charset=utf-8" },
	});
}

function isValidAbsoluteUrl(maybeUrl: string): boolean {
	try {
		// eslint-disable-next-line no-new
		new URL(maybeUrl);
		return true;
	} catch {
		return false;
	}
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		void env;

		const url = new URL(request.url);
		const upstreamUrl = buildUpstreamMetadataUrl(url);
		if (!upstreamUrl) {
			return notFoundWithContext("Request path did not contain a /metadata/ segment, so it could not be proxied.", {
				requestUrl: url.toString(),
				pathname: url.pathname,
				expected: "A path containing '/metadata/' (e.g. /java/metadata/ga/... or /metadata/ga/...)",
			});
		}

		// Special-case: ?direct + .json => fetch upstream JSON and redirect to first tar.gz/zip url.
		if (!isDirectJsonRequest(url)) {
			// Everything else just proxies to the upstream metadata.
			return fetch(new Request(upstreamUrl, request));
		}

		// Fetch canonical JSON (remove ?direct before going upstream).
		upstreamUrl.searchParams.delete("direct");

		let upstreamRes: Response;
		try {
			upstreamRes = await fetch(upstreamUrl.toString(), {
				method: "GET",
				headers: { accept: "application/json" },
			});
		} catch (err) {
			return notFoundWithContext("Upstream fetch failed.", {
				requestUrl: url.toString(),
				upstreamUrl: upstreamUrl.toString(),
				error: err instanceof Error ? `${err.name}: ${err.message}` : String(err),
			});
		}

		if (!upstreamRes.ok) {
			return notFoundWithContext("Upstream returned non-OK status for JSON metadata.", {
				requestUrl: url.toString(),
				upstreamUrl: upstreamUrl.toString(),
				upstreamStatus: upstreamRes.status,
				upstreamStatusText: upstreamRes.statusText,
				upstreamContentType: upstreamRes.headers.get("content-type"),
			});
		}

		let json: unknown;
		try {
			json = await upstreamRes.json();
		} catch (err) {
			return notFoundWithContext("Upstream response was not valid JSON.", {
				requestUrl: url.toString(),
				upstreamUrl: upstreamUrl.toString(),
				upstreamContentType: upstreamRes.headers.get("content-type"),
				error: err instanceof Error ? `${err.name}: ${err.message}` : String(err),
			});
		}

		const match = findFirstDirectMatch(json);
		if (!match) {
			return notFoundWithContext('No entry found with `file_type` of "tar.gz" or "zip" and a `url` field.', {
				requestUrl: url.toString(),
				upstreamUrl: upstreamUrl.toString(),
			});
		}

		if (!isValidAbsoluteUrl(match.url)) {
			return notFoundWithContext("Matched entry had an invalid URL.", {
				requestUrl: url.toString(),
				upstreamUrl: upstreamUrl.toString(),
				match,
			});
		}

		return new Response(null, {
			status: 302,
			headers: {
				location: match.url,
				"cache-control": "no-store",
			},
		});
	},
};
