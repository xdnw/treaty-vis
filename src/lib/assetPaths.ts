function normalizeBaseUrl(baseUrl: string): string {
  if (!baseUrl) {
    return "/";
  }
  return baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
}

export function withBasePath(relativePath: string): string {
  const baseUrl = normalizeBaseUrl(import.meta.env.BASE_URL ?? "/");
  const normalizedPath = relativePath.startsWith("/") ? relativePath.slice(1) : relativePath;
  return `${baseUrl}${normalizedPath}`;
}

export function dataAssetPath(fileName: string): string {
  return withBasePath(`data/${fileName}`);
}

export function normalizeDataAssetUrl(rawUrl: string, fallbackFileName: string): string {
  const trimmed = rawUrl.trim();
  if (!trimmed) {
    return dataAssetPath(fallbackFileName);
  }

  const isAbsolute = /^(?:[a-z][a-z\d+.-]*:)?\/\//i.test(trimmed);
  if (isAbsolute || trimmed.startsWith("data:") || trimmed.startsWith("blob:")) {
    return trimmed;
  }

  if (trimmed.startsWith("/")) {
    return withBasePath(trimmed);
  }

  if (trimmed.startsWith("data/")) {
    return withBasePath(trimmed);
  }

  if (!trimmed.includes("/")) {
    return dataAssetPath(trimmed);
  }

  return withBasePath(trimmed);
}
