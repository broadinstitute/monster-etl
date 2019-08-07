#!/usr/bin/env bash

set -euo pipefail

declare -r source_bucket="gs://broad-dsp-monster-v2f-prod-ingest-staging/inputs"
declare -r destination_bucket="gs://broad-dsp-monster-v2f-dev-ingest-storage/test/inputs"
declare -r frequency_analysis="frequency-analysis"
declare -r meta_analysis_as="meta-analysis/ancestry-specific"
declare -r meta_analysis_te="meta-analysis/trans-ethnic"
declare -r variant_effect_rfc="variant-effect/regulatory-feature-consequences"
declare -r variant_effect_tc="variant-effect/transcript-consequences"

# 1 common phenotype across all sections with phenotypes, 1 unique phenotype per section (variants don't have phenotype)
declare -r phenotype_across="CHOL"
declare -r phenotype_fa="Alb"
declare -r phenotype_as="BMI"
declare -r phenotype_te="CAD"

# copy frequency-analysis files from prod to dev-test
gsutil -m cp $(gsutil ls "$source_bucket/$frequency_analysis/$phenotype_across/" | head -n 2) \
"$destination_bucket/$frequency_analysis/$phenotype_across/"
gsutil -m cp $(gsutil ls "$source_bucket/$frequency_analysis/$phenotype_fa/" | head -n 2) \
"$destination_bucket/$frequency_analysis/$phenotype_fa/"

# copy meta-analysis/ancestry-specific files from prod to dev-test
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_across/ancestry=AA/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_across/ancestry=AA/"
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_across/ancestry=EA/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_across/ancestry=EA/"
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_across/ancestry=EU/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_across/ancestry=EU/"
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_across/ancestry=HS/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_across/ancestry=HS/"
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_across/ancestry=SA/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_across/ancestry=SA/"

gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_as/ancestry=AA/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_as/ancestry=AA/"
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_as/ancestry=EA/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_as/ancestry=EA/"
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_as/ancestry=EU/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_as/ancestry=EU/"
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_as/ancestry=HS/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_as/ancestry=HS/"
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_as/$phenotype_as/ancestry=SA/" | head -n 2) \
"$destination_bucket/$meta_analysis_as/$phenotype_as/ancestry=SA/"

# copy meta-analysis/trans-ethnic files from prod to dev-test
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_te/$phenotype_across/" | head -n 2) \
"$destination_bucket/$meta_analysis_te/$phenotype_across/"
gsutil -m cp $(gsutil ls "$source_bucket/$meta_analysis_te/$phenotype_te/" | head -n 2) \
"$destination_bucket/$meta_analysis_te/$phenotype_te/"

# copy variant-effect/regulatory-feature-consequences files from prod to dev-test
gsutil -m cp $(gsutil ls "$source_bucket/$variant_effect_rfc/" | head -n 2) \
"$destination_bucket/$variant_effect_rfc/"

# copy variant-effect/transcript-consequences from prod to dev-test
gsutil -m cp $(gsutil ls "$source_bucket/$variant_effect_tc/" | head -n 2) \
"$destination_bucket/$variant_effect_tc/"
