import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

const webData = path.resolve(process.cwd(), "public", "data");

const requiredFiles = [
  "treaty_changes_reconciled.msgpack",
  "treaty_changes_reconciled_summary.msgpack",
  "treaty_changes_reconciled_flags.msgpack",
  "flags.msgpack",
  "flag_assets.msgpack",
  "flag_atlas.webp",
  "flag_atlas.png",
  "alliance_score_ranks_daily.msgpack",
  "alliance_scores_v2.msgpack"
];

const optionalFiles = ["treaty_frame_index_v1.msgpack"];

async function fileHash(filePath) {
  const data = await fs.readFile(filePath);
  return crypto.createHash("sha256").update(data).digest("hex");
}

// Ensure dataset hashing is deterministic across runtimes by sorting object keys recursively.
function stableStringify(value) {
  function normalize(input) {
    if (Array.isArray(input)) {
      return input.map((item) => normalize(item));
    }

    if (Object.prototype.toString.call(input) === "[object Object]") {
      return Object.keys(input)
        .sort()
        .reduce((acc, key) => {
          acc[key] = normalize(input[key]);
          return acc;
        }, {});
    }

    return input;
  }

  return JSON.stringify(normalize(value));
}

async function main() {
  await fs.mkdir(webData, { recursive: true });

  const manifest = {
    generatedAt: new Date().toISOString(),
    files: {}
  };

  for (const fileName of requiredFiles) {
    const target = path.join(webData, fileName);
    const stat = await fs.stat(target);

    manifest.files[fileName] = {
      sizeBytes: stat.size,
      sha256: await fileHash(target)
    };
  }

  for (const fileName of optionalFiles) {
    const target = path.join(webData, fileName);
    let stat;
    try {
      stat = await fs.stat(target);
    } catch (error) {
      if (error && error.code === "ENOENT") {
        continue;
      }
      throw error;
    }

    manifest.files[fileName] = {
      sizeBytes: stat.size,
      sha256: await fileHash(target)
    };
  }

  const idSource = stableStringify(manifest.files);
  manifest.datasetId = crypto.createHash("sha256").update(idSource).digest("hex").slice(0, 16);

  await fs.writeFile(path.join(webData, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  console.log(`Refreshed manifest in ${webData}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
