import { z } from "zod";

const confidenceSchema = z.enum(["low", "medium", "high"]).catch("medium");

export const timelapseEventSchema = z.object({
  event_id: z.string(),
  timestamp: z.string(),
  action: z.string(),
  treaty_type: z.string(),
  from_alliance_id: z.number(),
  from_alliance_name: z.string().nullish().transform((value) => value ?? ""),
  to_alliance_id: z.number(),
  to_alliance_name: z.string().nullish().transform((value) => value ?? ""),
  pair_min_id: z.number(),
  pair_max_id: z.number(),
  source: z.string().nullish().default("unknown"),
  source_ref: z.string().nullish().default(""),
  confidence: confidenceSchema,
  inferred: z.boolean(),
  inference_reason: z.string().nullable(),
  time_remaining_turns: z.number().nullable(),
  grounded_from: z.boolean().catch(false),
  grounded_to: z.boolean().catch(false),
  grounded_keep: z.boolean().catch(true),
  noise_filtered: z.boolean().catch(false),
  noise_reason: z.string().nullish().transform((value) => value ?? null)
});

export const summarySchema = z.object({
  generated_at: z.string(),
  parameters: z.record(z.union([z.string(), z.number(), z.boolean()])),
  events_total: z.number(),
  flags_total: z.number(),
  counts_by_action: z.record(z.number()),
  counts_by_type: z.record(z.number()),
  counts_by_source: z.record(z.number())
});

export const flagSchema = z.record(z.any());

export const allianceFlagActionSchema = z.enum(["initial", "created", "changed"]);

export const allianceFlagEventSchema = z.object({
  timestamp: z.string(),
  action: allianceFlagActionSchema,
  alliance_id: z.number(),
  alliance_name: z.string().nullish().transform((value) => value ?? ""),
  flag_key: z.string().nullish().transform((value) => value ?? ""),
  previous_flag_key: z.string().nullish().transform((value) => value ?? ""),
  source_ref: z.string().nullish().transform((value) => value ?? "")
});

export const allianceFlagsPayloadSchema = z.object({
  events: z.array(allianceFlagEventSchema)
});

export const flagAssetEntrySchema = z.object({
  x: z.number(),
  y: z.number(),
  w: z.number(),
  h: z.number(),
  hash: z.string().nullish().transform((value) => value ?? "")
});

export const flagAssetsPayloadSchema = z.object({
  atlas: z.object({
    webp: z.string(),
    png: z.string().nullish().transform((value) => value ?? ""),
    width: z.number(),
    height: z.number(),
    tile_width: z.number(),
    tile_height: z.number()
  }),
  assets: z.record(flagAssetEntrySchema)
});

export const manifestSchema = z.object({
  generatedAt: z.string(),
  datasetId: z.string(),
  files: z.record(
    z.object({
      sizeBytes: z.number(),
      sha256: z.string()
    })
  )
});

const allianceScoreDaySchema = z.record(z.coerce.number());

export const allianceScoresDailySchema = z.object({
  scores_by_day: z.record(allianceScoreDaySchema)
});

export type AllianceScoresByDay = z.infer<typeof allianceScoresDailySchema>["scores_by_day"];

export type TimelapseEvent = z.infer<typeof timelapseEventSchema>;
export type TimelapseSummary = z.infer<typeof summarySchema>;
export type TimelapseManifest = z.infer<typeof manifestSchema>;
export type TimelapseFlag = z.infer<typeof flagSchema>;
export type AllianceScoresDaily = z.infer<typeof allianceScoresDailySchema>;
export type AllianceFlagAction = z.infer<typeof allianceFlagActionSchema>;
export type AllianceFlagEvent = z.infer<typeof allianceFlagEventSchema>;
export type AllianceFlagsPayload = z.infer<typeof allianceFlagsPayloadSchema>;
export type FlagAssetEntry = z.infer<typeof flagAssetEntrySchema>;
export type FlagAssetsPayload = z.infer<typeof flagAssetsPayloadSchema>;

export type AllianceFlagTimelineEntry = {
  timestamp: string;
  day: string;
  allianceName: string;
  action: AllianceFlagAction;
  flagKey: string;
  sourceRef: string;
};

export type AllianceFlagTimelineByAlliance = Record<string, AllianceFlagTimelineEntry[]>;

export type AllianceFlagSnapshot = {
  flagKey: string;
  action: AllianceFlagAction;
  timestamp: string;
  day: string;
  allianceName: string;
};
