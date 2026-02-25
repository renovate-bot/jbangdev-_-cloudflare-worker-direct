import { Env } from "./external";

type DirectMatch = {
	url: string;
	fileType: string;
	jsonPath: string;
};

const DEFAULT_DIRECT_FILE_TYPES = ["tar.gz", "zip"] as const;

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

function normalizeDirectFileType(raw: string): string {
	const normalized = raw.trim().toLowerCase().replace(/^\.+/, "");
	if (normalized === "tgz") return "tar.gz";
	return normalized;
}

function getDirectFileTypePreferences(url: URL): string[] {
	const directParam = url.searchParams.get("direct");
	if (directParam === null || directParam.trim() === "") {
		return [...DEFAULT_DIRECT_FILE_TYPES];
	}

	const prefs: string[] = [];
	for (const part of directParam.split(",")) {
		const fileType = normalizeDirectFileType(part);
		if (!fileType || prefs.includes(fileType)) continue;
		prefs.push(fileType);
	}

	return prefs.length > 0 ? prefs : [...DEFAULT_DIRECT_FILE_TYPES];
}

function findPreferredDirectMatch(value: unknown, preferredFileTypes: string[]): DirectMatch | null {
	const targetTypes = new Set(preferredFileTypes);
	const firstByType = new Map<string, DirectMatch>();
	const seen = new Set<unknown>();

	const walk = (node: unknown, path: string): void => {
		if (node === null || typeof node !== "object") return;
		if (seen.has(node)) return;
		seen.add(node);

		if (Array.isArray(node)) {
			for (let i = 0; i < node.length; i++) {
				walk(node[i], `${path}[${i}]`);
			}
			return;
		}

		const obj = node as Record<string, unknown>;
		const fileType = typeof obj.file_type === "string" ? obj.file_type.toLowerCase() : null;
		const url = typeof obj.url === "string" ? obj.url : null;

		if (fileType && url && targetTypes.has(fileType)) {
			if (!firstByType.has(fileType)) {
				firstByType.set(fileType, { url, fileType, jsonPath: path });
			}
		}

		for (const [k, v] of Object.entries(obj)) {
			walk(v, path ? `${path}.${k}` : k);
		}
	};

	walk(value, "");
	for (const fileType of preferredFileTypes) {
		const match = firstByType.get(fileType);
		if (match) return match;
	}
	return null;
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

		// Special-case: ?direct + .json => fetch upstream JSON and redirect to preferred archive url.
		if (!isDirectJsonRequest(url)) {
			// Everything else just proxies to the upstream metadata.
			return fetch(new Request(upstreamUrl, request));
		}

		const preferredFileTypes = getDirectFileTypePreferences(url);

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

		const match = findPreferredDirectMatch(json, preferredFileTypes);
		if (!match) {
			return notFoundWithContext("No matching entry found for requested `direct` archive preference(s).", {
				requestUrl: url.toString(),
				upstreamUrl: upstreamUrl.toString(),
				preferredFileTypes,
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
