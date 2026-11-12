#!/usr/bin/env python3
"""Generate synthetic Explicit VR Little Endian DICOM sample files.

Two scenarios are materialized under samples/generated/ :

1. cross_center_uid_collision/
   Centers ``center-a`` and ``center-b`` independently reused the *same*
   original SOP/Study/Series UIDs for completely different pixel content.
   The platform must assign different remapped UIDs per center so the
   segmentation / references can never be wired to the other center's image.

2. reference_first/
   ``struct_001.dcm`` (an SR-like object with nested ReferencedSOPSequence)
   references an image that has not been uploaded yet. Uploading it first
   creates a pending reference; the later ``image_001.dcm`` upload resolves it.

3. invalid/
   A non-DICOM blob used to demonstrate whole-batch rejection.

All generated objects are valid media-storage files with the DICM preamble,
file meta information, transfer syntax 1.2.840.10008.1.2.1 and (for images)
real PixelData. Run with:  python3 scripts/generate_samples.py
"""
from __future__ import annotations

import sys
from pathlib import Path

from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian

CT_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.2"          # CT Image Storage
SR_SOP_CLASS = "1.2.840.10008.5.1.4.1.1.88.11"      # Basic Text SR Storage

# UIDs deliberately reused by both centers. Components are numeric only, as
# required by DICOM PS3.5 Table 6.2-1.
COLLISION_STUDY_UID = "1.9.84.20261112.10.1"
COLLISION_SERIES_UID = "1.9.84.20261112.20.1"
COLLISION_SOP_UID = "1.9.84.20261112.30.100"

# Reference-first scenario.
REF_STUDY_UID = "1.9.84.20261112.40.1"
REF_SERIES_UID = "1.9.84.20261112.50.1"
REF_TARGET_SOP_UID = "1.9.84.20261112.60.200"
REF_STRUCT_SOP_UID = "1.9.84.20261112.60.201"

# Body-part dictionary scenario. The same anatomical regions are spelled
# differently (or omitted) across centers. Each file below has a unique SOP.
BP_STUDY_UID = "1.9.84.20261112.80.1"
BP_SERIES_UID = "1.9.84.20261112.81.1"
BP_SOP_BASE = "1.9.84.20261112.82."


def make_body_part_samples(root: Path) -> None:
    """Create one object per (center, raw BodyPartExamined) spelling.

    * center-a uses uppercase DICOM-style words;
    * center-b uses pinyin abbreviations and a local code;
    * center-c sends a lowercase spelling, an empty value and an object where
      the attribute is absent entirely.

    Raw values deliberately collide semantically but never textually across
    centers (note case-sensitive ``CHEST`` vs ``chest``), so an exact-match
    per-center dictionary is required to unify them.
    """
    cases = [
        # (center, file name, sop suffix, raw value, pixel multiplier)
        ("center-a", "a_chest_01.dcm", 101, "CHEST"),
        ("center-a", "a_hn_01.dcm", 102, "NECK"),
        ("center-a", "a_unknown_01.dcm", 103, "TORSO"),  # unmapped raw value
        ("center-b", "b_xiong_01.dcm", 201, "XIONG"),
        ("center-b", "b_headneck_01.dcm", 202, "HN"),
        ("center-c", "c_chest_01.dcm", 301, "chest"),
        ("center-c", "c_empty_01.dcm", 302, ""),        # empty value
    ]
    for center, name, suffix, raw in cases:
        make_ct_image(
            root / "bp-dict" / center / name,
            study_uid=BP_STUDY_UID,
            series_uid=BP_SERIES_UID,
            sop_uid=f"{BP_SOP_BASE}{suffix}",
            pixel_value=suffix * 10,
            body_part_examined=raw,
        )
    # Attribute entirely absent -> also UNCLASSIFIED.
    make_ct_image(
        root / "bp-dict" / "center-c" / "c_missing_01.dcm",
        study_uid=BP_STUDY_UID,
        series_uid=BP_SERIES_UID,
        sop_uid=f"{BP_SOP_BASE}303",
        pixel_value=3030,
        body_part_examined=None,
    )


def _file_meta(sop_class_uid: str, sop_instance_uid: str) -> FileMetaDataset:
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = sop_class_uid
    meta.MediaStorageSOPInstanceUID = sop_instance_uid
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    return meta


def _base_dataset(path: str, meta: FileMetaDataset) -> FileDataset:
    # Explicit VR Little Endian is implied by the Transfer Syntax UID in the
    # file meta; pydicom 3 derives the endianness/VR flags from it.
    ds = FileDataset(path, {}, file_meta=meta, preamble=b"\0" * 128)
    ds.ContentDate = "20261112"
    ds.ContentTime = "120000"
    ds.PatientName = "Synthetic^Patient"
    ds.PatientID = "SYN-0001"
    return ds


def make_ct_image(
    path: Path,
    *,
    study_uid: str,
    series_uid: str,
    sop_uid: str,
    pixel_value: int,
    modality: str = "CT",
    body_part_examined: str | None = None,
) -> Path:
    ds = _base_dataset(str(path), _file_meta(CT_SOP_CLASS, sop_uid))
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = series_uid
    ds.SOPInstanceUID = sop_uid
    ds.Modality = modality
    if body_part_examined is not None:
        # (0018,0015) Code String. Written verbatim so each center can use its
        # own spelling/casing for the body-part dictionary demo.
        ds.BodyPartExamined = body_part_examined
    ds.SeriesNumber = 1
    ds.InstanceNumber = 1
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.Rows = 4
    ds.Columns = 4
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    # Distinct, deterministic pixel content per center.
    ds.PixelData = (pixel_value.to_bytes(2, "little")) * (ds.Rows * ds.Columns)
    path.parent.mkdir(parents=True, exist_ok=True)
    ds.save_as(path, enforce_file_format=True)
    return path


def make_sr_with_reference(
    path: Path,
    *,
    study_uid: str,
    series_uid: str,
    sop_uid: str,
    referenced_sop_uid: str,
    referenced_series_uid: str,
) -> Path:
    ds = _base_dataset(str(path), _file_meta(SR_SOP_CLASS, sop_uid))
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = series_uid
    ds.SOPInstanceUID = sop_uid
    ds.Modality = "SR"
    ds.SeriesNumber = 2
    ds.InstanceNumber = 1

    # Nested reference: ReferencedSOPSequence -> ReferencedSeriesSequence ->
    # ReferencedSOPSequence. Recursive rewriting must reach every depth.
    inner_ref = Dataset()
    inner_ref.ReferencedSOPClassUID = CT_SOP_CLASS
    inner_ref.ReferencedSOPInstanceUID = referenced_sop_uid

    ref_series = Dataset()
    ref_series.SeriesInstanceUID = referenced_series_uid
    ref_series.ReferencedSOPSequence = [inner_ref]

    top_ref = Dataset()
    top_ref.ReferencedSOPClassUID = CT_SOP_CLASS
    top_ref.ReferencedSOPInstanceUID = referenced_sop_uid
    top_ref.ReferencedSeriesSequence = [ref_series]

    ds.ReferencedSOPSequence = [top_ref]
    path.parent.mkdir(parents=True, exist_ok=True)
    ds.save_as(path, enforce_file_format=True)
    return path


def generate(root: Path) -> None:
    # --- Scenario 1: cross-center UID collision, different pixel bytes. ---
    make_ct_image(
        root / "cross_center_uid_collision" / "center-a" / "image_001.dcm",
        study_uid=COLLISION_STUDY_UID,
        series_uid=COLLISION_SERIES_UID,
        sop_uid=COLLISION_SOP_UID,
        pixel_value=1000,
    )
    make_ct_image(
        root / "cross_center_uid_collision" / "center-b" / "image_001.dcm",
        study_uid=COLLISION_STUDY_UID,
        series_uid=COLLISION_SERIES_UID,
        sop_uid=COLLISION_SOP_UID,
        pixel_value=2000,  # same UIDs, different bytes -> different center
    )

    # --- Scenario 2: reference arrives before the referenced image. ---
    make_sr_with_reference(
        root / "reference_first" / "struct_001.dcm",
        study_uid=REF_STUDY_UID,
        series_uid=REF_SERIES_UID,
        sop_uid=REF_STRUCT_SOP_UID,
        referenced_sop_uid=REF_TARGET_SOP_UID,
        referenced_series_uid=REF_SERIES_UID,
    )
    make_ct_image(
        root / "reference_first" / "image_001.dcm",
        study_uid=REF_STUDY_UID,
        series_uid=REF_SERIES_UID,
        sop_uid=REF_TARGET_SOP_UID,
        pixel_value=1500,
    )

    # --- Scenario 3: invalid object for whole-batch rejection. ---
    invalid = root / "invalid" / "not_a_dicom.dcm"
    invalid.parent.mkdir(parents=True, exist_ok=True)
    invalid.write_bytes(b"this is definitely not a DICOM file\n" * 4)

    # --- Scenario 4: heterogeneous BodyPartExamined spellings per center. ---
    make_body_part_samples(root)

    for path in sorted(root.rglob("*")):
        if path.is_file():
            print(f"generated {path.relative_to(root)} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    out_root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else (
        Path(__file__).resolve().parents[1] / "samples" / "generated"
    )
    generate(out_root)
