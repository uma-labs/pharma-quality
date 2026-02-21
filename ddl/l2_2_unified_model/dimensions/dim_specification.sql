-- =============================================================================
-- Table  : dim_specification
-- Schema : l2_2_spec_unified
-- Layer  : L2.2 — Unified Data Model (Business Conform Layer)
-- Domain : Pharmaceutical Quality — Specifications
-- Grain  : One row per specification version (SCD Type 2)
-- Purpose: Specification header / metadata dimension. Captures the identity,
--          regulatory context, lifecycle status, and product/material linkage
--          of each pharmaceutical specification document.
-- CTD    : Supports 3.2.S.4.1 (Drug Substance) and 3.2.P.5.1 (Drug Product)
-- Author : Pharma Quality Data Team
-- =============================================================================

CREATE TABLE IF NOT EXISTS l2_2_spec_unified.dim_specification
(
    -- -------------------------------------------------------------------------
    -- Keys
    -- -------------------------------------------------------------------------
    spec_key                    BIGINT          NOT NULL    GENERATED ALWAYS AS IDENTITY
                                                            COMMENT 'Surrogate primary key (system-generated)',

    spec_id                     STRING          NOT NULL    COMMENT 'Natural / business key from source system',

    -- -------------------------------------------------------------------------
    -- Specification Identity
    -- -------------------------------------------------------------------------
    spec_number                 STRING          NOT NULL    COMMENT 'Specification document number (e.g., SP-DS-2024-001)',
    spec_version                STRING          NOT NULL    COMMENT 'Version string (e.g., 1.0, 2.1, 3.0)',
    spec_title                  STRING                      COMMENT 'Full specification document title',

    -- -------------------------------------------------------------------------
    -- Specification Type
    -- spec_type_code values:
    --   DS      = Drug Substance (Active Pharmaceutical Ingredient)
    --   DP      = Drug Product (Finished Dosage Form)
    --   RM      = Raw Material
    --   EXCIP   = Excipient
    --   INTERMED= Intermediate
    --   IPC     = In-Process Control
    --   CCS     = Container Closure System
    -- -------------------------------------------------------------------------
    spec_type_code              STRING          NOT NULL    COMMENT 'Type code: DS|DP|RM|EXCIP|INTERMED|IPC|CCS',
    spec_type_name              STRING                      COMMENT 'Type display name',

    -- -------------------------------------------------------------------------
    -- Foreign Keys (Dimensions)
    -- -------------------------------------------------------------------------
    product_key                 BIGINT                      COMMENT 'FK to dim_product (NULL for drug substance/raw material specs)',
    material_key                BIGINT                      COMMENT 'FK to dim_material',
    regulatory_context_key      BIGINT                      COMMENT 'FK to dim_regulatory_context',

    -- -------------------------------------------------------------------------
    -- Regulatory / CTD Attributes
    -- -------------------------------------------------------------------------
    ctd_section                 STRING                      COMMENT 'CTD section reference (e.g., 3.2.S.4.1, 3.2.P.5.1, 3.2.P.4)',

    -- stage_code values:
    --   DEV = Development (pre-IND)
    --   CLI = Clinical (Phase I-III, IND)
    --   COM = Commercial (post-approval, NDA/MAA)
    stage_code                  STRING                      COMMENT 'Lifecycle stage: DEV|CLI|COM',
    stage_name                  STRING                      COMMENT 'Stage display name',

    -- -------------------------------------------------------------------------
    -- Specification Status (Lifecycle)
    -- status_code values:
    --   DRA  = Draft
    --   APP  = Approved (effective, current)
    --   SUP  = Superseded (replaced by newer version)
    --   OBS  = Obsolete (no longer valid)
    --   ARCH = Archived
    -- -------------------------------------------------------------------------
    status_code                 STRING          NOT NULL    COMMENT 'Status code: DRA|APP|SUP|OBS|ARCH',
    status_name                 STRING                      COMMENT 'Status display name',

    -- -------------------------------------------------------------------------
    -- Dates
    -- -------------------------------------------------------------------------
    effective_date              DATE                        COMMENT 'Date specification became effective / active',
    expiry_date                 DATE                        COMMENT 'Scheduled expiry date (e.g., for periodic review)',
    approval_date               DATE                        COMMENT 'Date of formal quality/regulatory approval',

    -- -------------------------------------------------------------------------
    -- Approval / Authorization
    -- -------------------------------------------------------------------------
    approver_name               STRING                      COMMENT 'Name of approving authority',
    approver_title              STRING                      COMMENT 'Title/role of approving authority',

    -- -------------------------------------------------------------------------
    -- Site
    -- -------------------------------------------------------------------------
    site_code                   STRING                      COMMENT 'Manufacturing or testing site code',
    site_name                   STRING                      COMMENT 'Manufacturing or testing site name',

    -- -------------------------------------------------------------------------
    -- Compendia Reference
    -- compendia_reference values: USP, EP, JP, BP, ACS, NF
    -- -------------------------------------------------------------------------
    compendia_reference         STRING                      COMMENT 'Pharmacopoeia basis: USP|EP|JP|BP|NF',

    -- -------------------------------------------------------------------------
    -- Versioning Linkage
    -- -------------------------------------------------------------------------
    supersedes_spec_id          STRING                      COMMENT 'Natural key of the prior specification version superseded by this record',

    -- -------------------------------------------------------------------------
    -- Source System Provenance
    -- -------------------------------------------------------------------------
    source_system_code          STRING          NOT NULL    COMMENT 'Source system identifier: LIMS|SAP|VAULT|MANUAL',
    source_system_id            STRING                      COMMENT 'Original record identifier in source system',

    -- -------------------------------------------------------------------------
    -- SCD Type 2 Tracking
    -- -------------------------------------------------------------------------
    load_timestamp              TIMESTAMP       NOT NULL    COMMENT 'ETL batch load timestamp (UTC)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'TRUE = current/active row; FALSE = historical row (SCD2)',
    valid_from                  TIMESTAMP       NOT NULL    COMMENT 'SCD2 row validity start timestamp',
    valid_to                    TIMESTAMP                   COMMENT 'SCD2 row validity end timestamp (NULL = currently active)'
)
USING DELTA
PARTITIONED BY (spec_type_code)
COMMENT 'L2.2 Specification header dimension. One row per specification version (SCD Type 2). Captures identity, lifecycle status, regulatory context, and product/material linkage. Supports CTD 3.2.S.4.1 and 3.2.P.5.1.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.enableChangeDataFeed'        = 'true',
    'quality.domain'                    = 'specifications',
    'quality.layer'                     = 'L2.2',
    'quality.scd_type'                  = '2',
    'quality.grain'                     = 'specification_version',
    'quality.ctd_sections'              = '3.2.S.4.1,3.2.P.5.1'
);

-- Optimize read performance on common filter patterns
-- OPTIMIZE l2_2_spec_unified.dim_specification ZORDER BY (spec_number, spec_version);

-- -------------------------------------------------------------------------
-- SPEC_TYPE_CODE Reference Values
-- -------------------------------------------------------------------------
-- DS       Drug Substance (API)
-- DP       Drug Product (Finished Dosage Form)
-- RM       Raw Material
-- EXCIP    Excipient
-- INTERMED Intermediate
-- IPC      In-Process Control
-- CCS      Container Closure System

-- -------------------------------------------------------------------------
-- STAGE_CODE Reference Values
-- -------------------------------------------------------------------------
-- DEV   Development / Pre-clinical
-- CLI   Clinical (IND phase)
-- COM   Commercial (post-NDA/MAA approval)

-- -------------------------------------------------------------------------
-- STATUS_CODE Lifecycle Transitions
-- -------------------------------------------------------------------------
-- DRA → APP → SUP → OBS
--           → ARCH
