export interface StepProgress {
  name: string
  status: string
  processed: number
  total: number
  percent: number
}

export interface CrawlProgress {
  status: string
  job_id: number | null
  started_at: number | null
  elapsed: number
  message: string
  steps: Record<string, StepProgress>
}

export interface GroupInfo {
  group_number: number
  group_label: string
  count: number
}

export interface Summary {
  total: number
  groups: GroupInfo[]
  duplicates_exact: { dup_hash: string; cnt: number; bm_ids: number[] }[]
  duplicates_normalized: { dup_hash: string; cnt: number; bm_ids: number[] }[]
  duplicates_structure: { dup_hash: string; cnt: number; bm_ids: number[] }[]
  duplicates_names: { name: string; cnt: number; bm_ids: number[] }[]
  top_dirty_datasets: { dataset_id: string; url: string; total: number; unused: number; cleanup_candidates: number }[]
}

export interface BmRow {
  bm_id: number
  bm_name: string
  legacy_id: string
  group_number: number
  group_label: string
  active_cards_count: number
  total_views: number
  referenced_by_count: number
  dataset_names: string
  naming_flag: string
  complexity_score: number
  url: string
  owner_name?: string
  card_ids?: string
}
