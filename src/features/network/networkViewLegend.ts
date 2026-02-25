type EdgeLegendItem = {
  key: string;
  label: string;
  color: string;
};

export const TREATY_TYPE_STYLES: Record<string, { label: string; color: string }> = {
  EXTENSION: { label: "Extension", color: "#7c3aed" },
  MDOAP: { label: "MDOAP", color: "#0f766e" },
  MDP: { label: "MDP", color: "#0b7285" },
  NAP: { label: "NAP", color: "#2b8a3e" },
  NPT: { label: "NPT", color: "#1d4ed8" },
  ODOAP: { label: "ODOAP", color: "#9a3412" },
  ODP: { label: "ODP", color: "#5f3dc4" },
  OFFSHORE: { label: "Offshore", color: "#475569" },
  PIAT: { label: "PIAT", color: "#364fc7" },
  PROTECTORATE: { label: "Protectorate", color: "#d9480f" }
};

export const EDGE_FALLBACK_COLOR = "#4b5563";

export const EDGE_LEGEND_ITEMS: EdgeLegendItem[] = [
  ...Object.entries(TREATY_TYPE_STYLES).map(([key, style]) => ({
    key,
    color: style.color,
    label: style.label
  })),
  {
    key: "unknown",
    label: "Unknown / other",
    color: EDGE_FALLBACK_COLOR
  }
];
