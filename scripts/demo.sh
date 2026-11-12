#!/usr/bin/env bash
# End-to-end demo against a running ingest API (default: http://localhost:8000).
#
# It walks through every requirement using the synthetic samples under
# samples/generated/:
#   1. cross-center UID collision  (same original UID, two centers)
#   2. reference-first, then the referenced image arrives (same center)
#   3. reference must NOT resolve across centers
#   4. idempotent retransmit (same bytes) vs. byte conflict (whole-batch reject)
#   5. batch query + object download, verifying pixels / transfer syntax
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
SAMPLES="$(cd "$(dirname "$0")/../samples/generated" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
post_batch() { # center file...
  local center="$1"; shift
  curl -sS -X POST "$BASE_URL/api/v1/batches" \
    -F "center_id=$center" \
    $(printf -- '-F files=@%s ' "$@")
}

say "0. health check"
curl -sS "$BASE_URL/healthz"; echo

say "1. CROSS-CENTER UID COLLISION: both centers reuse the same original SOP UID"
post_batch center-a "$SAMPLES/cross_center_uid_collision/center-a/image_001.dcm" | tee "$WORK/a.json"; echo
post_batch center-b "$SAMPLES/cross_center_uid_collision/center-b/image_001.dcm" | tee "$WORK/b.json"; echo
UID_A=$(python3 -c 'import json;print(json.load(open("'$WORK/a.json'"))["files"][0]["new_sop_uid"])')
UID_B=$(python3 -c 'import json;print(json.load(open("'$WORK/b.json'"))["files"][0]["new_sop_uid"])')
[ "$UID_A" != "$UID_B" ] && echo "OK: same original UID -> distinct remapped UIDs per center"

say "2. REFERENCE-FIRST: upload the SR that points at an image not yet present"
post_batch center-x "$SAMPLES/reference_first/struct_001.dcm" | tee "$WORK/struct.json"; echo
echo "-- pending references for center-x (expect one unresolved):"
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-x&resolved=false"; echo

say "3. The SAME original target SOP uploaded by center-y must NOT resolve center-x's ref"
post_batch center-y "$SAMPLES/reference_first/image_001.dcm" > /dev/null
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-x&resolved=false"; echo
echo "  (still one pending row above: references never cross centers)"

say "4. Now center-x uploads its own image -> pending reference resolves"
post_batch center-x "$SAMPLES/reference_first/image_001.dcm" > /dev/null
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-x&resolved=false"; echo
echo "  (empty list above: reference resolved by same-center image)"

say "5. IDEMPOTENT RETRANSMIT: identical bytes reuse the stored object"
post_batch center-x "$SAMPLES/reference_first/image_001.dcm" | python3 -m json.tool

say "6. BYTE CONFLICT: same original SOP, changed bytes -> whole batch rejected (409)"
cp "$SAMPLES/reference_first/image_001.dcm" "$WORK/mutated.dcm"
printf '\001' | dd of="$WORK/mutated.dcm" bs=1 seek=$(( $(stat -c%s "$WORK/mutated.dcm") - 1 )) count=1 conv=notrunc 2>/dev/null
post_batch center-x "$WORK/mutated.dcm" "$SAMPLES/reference_first/struct_001.dcm"; echo

say "7. BATCH QUERY + DOWNLOAD: verify rewritten object keeps TS and pixel bytes"
BATCH=$(python3 -c 'import json;print(json.load(open("'$WORK/a.json'"))["batch_id"])')
curl -sS "$BASE_URL/api/v1/batches/$BATCH" | python3 -m json.tool
INSTANCE=$(curl -sS "$BASE_URL/api/v1/instances?center_id=center-a" | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["id"])')
curl -sS -D "$WORK/headers" "$BASE_URL/api/v1/instances/$INSTANCE/download" -o "$WORK/down.dcm"
grep -iE "content-type|x-new-sop|x-transfer" "$WORK/headers"
python3 - "$SAMPLES/cross_center_uid_collision/center-a/image_001.dcm" "$WORK/down.dcm" <<'PY'
import sys, pydicom
src, out = (pydicom.dcmread(p) for p in sys.argv[1:])
assert src.PixelData == out.PixelData, "pixel bytes changed!"
assert str(out.file_meta.TransferSyntaxUID) == "1.2.840.10008.1.2.1"
assert out.is_implicit_VR is False and out.is_little_endian is True
print("OK: pixel bytes identical; transfer syntax still Explicit VR Little Endian")
print("rewritten SOP:", out.SOPInstanceUID)
PY

say "8. INVALID OBJECT: whole batch rejected (422), nothing stored"
curl -sS -o "$WORK/inv.json" -w "HTTP %{http_code}\n" -X POST "$BASE_URL/api/v1/batches" \
  -F "center_id=center-z" \
  -F "files=@$SAMPLES/reference_first/image_001.dcm" \
  -F "files=@$SAMPLES/invalid/not_a_dicom.dcm"
cat "$WORK/inv.json"; echo
curl -sS "$BASE_URL/api/v1/batches?center_id=center-z"; echo "  (empty list above)"

# --------------------------------------------------------------------------- #
# Body-part dictionary replacement, cohort filtering and download
# --------------------------------------------------------------------------- #
BP="$SAMPLES/bp-dict"

say "9. BODY-PART INDEX NOT READY before the first dictionary (expect 503)"
curl -sS -o /dev/null -w "HTTP %{http_code}\n" \
  "$BASE_URL/api/v1/body-part/instances?center_id=center-a&standard_body_part=CHEST"

say "10. Ingest objects whose BodyPartExamined spelling differs per center"
for c in center-a center-b center-c; do
  post_batch "$c" "$BP/$c"/*.dcm > /dev/null
  echo "  $c ingested"
done

say "11. Submit the COMPLETE v1 dictionary -> background rebuild (202)"
cat > "$WORK/dict-v1.json" <<'JSON'
{
  "version_label": "bodypart-demo-v1",
  "entries": [
    {"center_id": "center-a", "original_value": "CHEST", "standard_body_part": "CHEST"},
    {"center_id": "center-a", "original_value": "NECK",  "standard_body_part": "HEAD-NECK"},
    {"center_id": "center-b", "original_value": "XIONG", "standard_body_part": "CHEST"},
    {"center_id": "center-b", "original_value": "HN",    "standard_body_part": "HEAD-NECK"},
    {"center_id": "center-c", "original_value": "chest", "standard_body_part": "CHEST"}
  ]
}
JSON
RID=$(curl -sS -X POST "$BASE_URL/api/v1/body-part/dictionaries" \
  -H 'Content-Type: application/json' --data @"$WORK/dict-v1.json" \
  | tee "$WORK/r1.json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
echo "rebuild id=$RID; polling until active..."
for _ in $(seq 1 100); do
  ST=$(curl -sS "$BASE_URL/api/v1/body-part/rebuilds/$RID" \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])')
  [ "$ST" != "building" ] && break
  sleep 0.2
done
echo "v1 rebuild -> $ST"
curl -sS "$BASE_URL/api/v1/body-part/active" | python3 -m json.tool

say "12. Cohort query: CHEST unifies center-a CHEST and center-b XIONG"
curl -sS "$BASE_URL/api/v1/body-part/instances?center_id=center-a&standard_body_part=CHEST" \
  | python3 -m json.tool
curl -sS "$BASE_URL/api/v1/body-part/instances?center_id=center-b&standard_body_part=CHEST" \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);print("center-b CHEST raw values:",[r["original_body_part"] for r in d])'

say "13. Unmapped / empty / missing values are all UNCLASSIFIED"
curl -sS "$BASE_URL/api/v1/body-part/instances?center_id=center-c&standard_body_part=UNCLASSIFIED" \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);print("center-c UNCLASSIFIED count:",len(d),"raw:",[r["original_body_part"] for r in d])'

say "14. Retransmit during active dictionary: reused, no duplicate index row"
post_batch center-c "$BP/center-c"/*.dcm \
  | python3 -c 'import json,sys;print("reused_count:",json.load(sys.stdin)["reused_count"])'

say "15. Download an object straight from the cohort query result"
DL=$(curl -sS "$BASE_URL/api/v1/body-part/instances?center_id=center-b&standard_body_part=CHEST" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["download_url"])')
echo "download_url=$DL"
curl -sS -o "$WORK/cohort.dcm" -w "HTTP %{http_code} %{content_type}\n" "$DL"
python3 - "$WORK/cohort.dcm" <<'PY'
import sys, pydicom
ds = pydicom.dcmread(sys.argv[1])
print("downloaded object BodyPartExamined:", ds.BodyPartExamined, "Modality:", ds.Modality)
PY

say "16. Replace with v2 (CHEST cohort renamed THORAX): atomic single-dictionary switch"
cat > "$WORK/dict-v2.json" <<'JSON'
{
  "version_label": "bodypart-demo-v2",
  "entries": [
    {"center_id": "center-a", "original_value": "CHEST", "standard_body_part": "THORAX"},
    {"center_id": "center-b", "original_value": "XIONG", "standard_body_part": "THORAX"},
    {"center_id": "center-c", "original_value": "chest", "standard_body_part": "THORAX"},
    {"center_id": "center-a", "original_value": "NECK",  "standard_body_part": "HEAD-NECK"},
    {"center_id": "center-b", "original_value": "HN",    "standard_body_part": "HEAD-NECK"}
  ]
}
JSON
RID2=$(curl -sS -X POST "$BASE_URL/api/v1/body-part/dictionaries" \
  -H 'Content-Type: application/json' --data @"$WORK/dict-v2.json" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
for _ in $(seq 1 100); do
  ST=$(curl -sS "$BASE_URL/api/v1/body-part/rebuilds/$RID2" \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])')
  [ "$ST" != "building" ] && break
  sleep 0.2
done
echo "v2 rebuild -> $ST"
echo -n "old CHEST label now returns: "
curl -sS "$BASE_URL/api/v1/body-part/instances?center_id=center-a&standard_body_part=CHEST"; echo
curl -sS "$BASE_URL/api/v1/body-part/instances?center_id=center-a&standard_body_part=THORAX" \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);print("new THORAX cohort:",[(r["standard_body_part"],r["dict_id"]) for r in d])'
echo "replacement history:"
curl -sS "$BASE_URL/api/v1/body-part/rebuilds" \
  | python3 -c 'import json,sys;[print(" ",r["id"],r["version_label"],r["status"]) for r in json.load(sys.stdin)]'

say "17. Single-flight: a second replacement while one runs is rejected (409)"
curl -sS -o /dev/null -w "duplicate version label -> HTTP %{http_code}\n" \
  -X POST "$BASE_URL/api/v1/body-part/dictionaries" \
  -H 'Content-Type: application/json' --data @"$WORK/dict-v2.json"

echo
echo "Interactive docs: open $BASE_URL/docs in a browser."
echo "Body-part endpoints are grouped under the 'body-part' tag."
