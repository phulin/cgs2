interface ContentBlock {
	type: string;
	label?: string;
	content?: string | null;
}

export interface StoredNodeContent {
	blocks: ContentBlock[];
}

export interface BreadcrumbNode {
	id: string;
	parent_id: string | null;
	heading_citation: string | null;
	name: string | null;
}

function collapseWhitespace(text: string): string {
	return text.replace(/\s+/g, " ").trim();
}

function formatBlock(block: ContentBlock): string {
	if (!block.content) {
		return "";
	}
	if (block.label) {
		return `${block.label}: ${block.content}`;
	}
	return block.content;
}

export function extractContentText(content: StoredNodeContent): string {
	return collapseWhitespace(
		content.blocks
			.map((block) => formatBlock(block))
			.filter((block) => block.length > 0)
			.join("\n\n"),
	);
}

export function buildTitleText(args: {
	headingCitation: string | null;
	name: string | null;
}): string {
	return collapseWhitespace(
		[args.headingCitation, args.name]
			.filter((value): value is string => Boolean(value))
			.join(" "),
	);
}

export function createBreadcrumbBuilder(nodes: BreadcrumbNode[]) {
	const byId = new Map(nodes.map((node) => [node.id, node]));
	const cache = new Map<string, string>();

	const build = (nodeId: string): string => {
		const cached = cache.get(nodeId);
		if (cached !== undefined) {
			return cached;
		}

		const node = byId.get(nodeId);
		if (!node) {
			cache.set(nodeId, "");
			return "";
		}

		const self = buildTitleText({
			headingCitation: node.heading_citation,
			name: node.name,
		});
		const parent =
			node.parent_id == null ? "" : build(node.parent_id).trim();

		const breadcrumb =
			parent && self ? `${parent} > ${self}` : parent || self;
		cache.set(nodeId, breadcrumb);
		return breadcrumb;
	};

	return build;
}

export function buildSearchText(args: {
	breadcrumb: string;
	titleText: string;
	bodyText: string;
}): string {
	return collapseWhitespace(
		[args.breadcrumb, args.titleText, args.bodyText]
			.filter((value) => value.length > 0)
			.join("\n"),
	);
}
