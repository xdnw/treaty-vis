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
