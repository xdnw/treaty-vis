import type { FlagAssetsPayload } from "@/domain/timelapse/schema";

export type NetworkAllianceHintData = {
  id: string;
  fullLabel: string;
  score: number | null;
  scoreDay: string | null;
  treatyCount: number;
  counterparties: number;
  flagKey: string | null;
  activeTreaties: Array<{
    counterpartyId: string;
    counterpartyLabel: string;
    treatyTypes: string[];
  }>;
};

type NetworkAllianceHintProps = {
  hint: NetworkAllianceHintData;
  flagAssetsPayload: FlagAssetsPayload | null;
  className?: string;
};

type FlagSpriteProps = {
  allianceLabel: string;
  flagKey: string;
  flagAssetsPayload: FlagAssetsPayload;
};

function FlagSprite({ allianceLabel, flagKey, flagAssetsPayload }: FlagSpriteProps) {
  const asset = flagAssetsPayload.assets[flagKey];
  if (!asset) {
    return <div className="text-slate-500">Flag unavailable</div>;
  }

  const atlas = flagAssetsPayload.atlas;
  const fallbackSrc = atlas.png || atlas.webp;

  return (
    <div
      className="inline-block overflow-hidden rounded border border-slate-300"
      style={{ width: asset.w, height: asset.h }}
      aria-label={`${allianceLabel} flag`}
      title={`${allianceLabel} flag`}
    >
      <picture>
        <source srcSet={atlas.webp} type="image/webp" />
        <img
          src={fallbackSrc}
          alt={`${allianceLabel} flag`}
          loading="lazy"
          className="block"
          style={{
            width: atlas.width,
            height: atlas.height,
            maxWidth: "none",
            maxHeight: "none",
            transform: `translate(-${asset.x}px, -${asset.y}px)`
          }}
        />
      </picture>
    </div>
  );
}

export function NetworkAllianceHint({ hint, flagAssetsPayload, className }: NetworkAllianceHintProps) {
  return (
    <div className={className ?? "rounded-md border border-slate-300 bg-slate-50 p-3 text-xs text-slate-700"}>
      <div className="text-[11px] uppercase tracking-wide text-slate-500">Alliance</div>
      <div className="text-sm font-semibold text-slate-900">{hint.fullLabel}</div>

      <div className="mt-2 text-[11px] uppercase tracking-wide text-slate-500">Score</div>
      <div>
        {typeof hint.score === "number" && Number.isFinite(hint.score)
          ? `${hint.score.toLocaleString(undefined, { maximumFractionDigits: 2 })}${hint.scoreDay ? ` (${hint.scoreDay})` : ""}`
          : "n/a"}
      </div>

      <div className="mt-2 text-[11px] uppercase tracking-wide text-slate-500">Flag</div>
      {hint.flagKey && flagAssetsPayload ? (
        <FlagSprite allianceLabel={hint.fullLabel} flagKey={hint.flagKey} flagAssetsPayload={flagAssetsPayload} />
      ) : (
        <div className="text-slate-500">Flag unavailable</div>
      )}

      <div className="mt-2 text-[11px] uppercase tracking-wide text-slate-500">Active Treaties / Counterparties</div>
      <div className="mb-1 text-slate-600">
        {hint.treatyCount} treaties across {hint.counterparties} counterparties
      </div>
      {hint.activeTreaties.length > 0 ? (
        <ul className="space-y-1">
          {hint.activeTreaties.map((treaty) => (
            <li key={`${hint.id}:${treaty.counterpartyId}`} className="leading-tight">
              <span className="font-medium">{treaty.counterpartyLabel}</span>: {treaty.treatyTypes.join(", ")}
            </li>
          ))}
        </ul>
      ) : (
        <div>none</div>
      )}
    </div>
  );
}
