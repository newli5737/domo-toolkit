/**
 * Monitor Service — Single source of truth cho Monitor API types & calls.
 */
import { apiGet, apiPost, apiDownload } from '../api'

// ─── Dataset / Dataflow ──────────────────────────────────

export interface DatasetRow {
  id: string
  name: string
  row_count: number
  column_count: number
  card_count: number
  data_flow_count: number
  provider_type: string
  stream_id: string
  schedule_state: string
  dataset_status: string
  last_execution_state: string
  last_updated: string
  updated_at: string
}

export interface DataflowRow {
  id: string
  name: string
  status: string
  paused: boolean
  database_type: string
  last_execution_time: string
  last_execution_state: string
  execution_count: number
  owner: string
  output_dataset_count: number
  updated_at: string
}

// ─── Health Check ────────────────────────────────────────

export interface HealthAlert {
  type: string
  status: string
  id: string
  name: string
  provider_type?: string
  database_type?: string
  card_count?: number
  last_updated?: string
  last_execution_time?: string
  last_execution_state?: string
  hours_ago?: number
}

export interface HealthCheckResult {
  summary: {
    datasets: { total_crawled: number; checked: number; ok: number; stale: number }
    dataflows: { total_crawled: number; checked: number; ok: number; failed: number; stale: number }
    total_alerts: number
  }
  alerts: HealthAlert[]
  checked_at: string
}

// ─── Crawl Status ────────────────────────────────────────

export interface JobStatusResponse {
  status: string
  message: string
  started_at?: string
}

export interface CrawlProgress {
  step: string
  processed: number
  total: number
  percent: number
}

export interface MonitorStatus {
  status: string
  result?: HealthCheckResult
  progress?: CrawlProgress
  started_at?: string
}

// ─── Alerts Page ─────────────────────────────────────────

export interface FailedDataset {
  id: string
  name: string
  provider_type: string
  last_execution_state: string
  card_count: number
}

export interface FailedDataflow {
  id: string
  name: string
  last_execution_state: string
  status?: string
}

export interface AlertData {
  checked_at: string | null
  all_ok: boolean
  failed_datasets: FailedDataset[]
  failed_dataflows: FailedDataflow[]
}

// ─── Auto-Check Config ───────────────────────────────────

export interface AutoCheckConfig {
  backlog_base_url: string
  backlog_issue_id: string
  has_backlog_cookie: boolean
  alert_email_to: string
  min_card_count: number
  provider_type: string
  has_gmail: boolean
  schedule_enabled: boolean
  schedule_hour: number
  schedule_minute: number
  schedule_days: string
}

export interface AutoCheckPayload {
  min_card_count: number
  provider_type: string
  alert_email: string
}

export interface SaveConfigPayload {
  alert_email: string
  min_card_count: number
  provider_type: string
  schedule_enabled: boolean
  schedule_hour: number
  schedule_minute: number
  schedule_days: string
}

export interface AutoCheckResult {
  all_ok: boolean
  error?: string
  backlog_posted?: boolean
  email_sent?: boolean
  failed_dataset_count: number
  failed_dataflow_count: number
}

export interface ProviderTypesResponse {
  provider_types: string[]
}

// ─── Service ─────────────────────────────────────────────

export const monitorService = {
  getDatasets: (limit = 2000) =>
    apiGet<{ total: number; datasets: DatasetRow[] }>(`/api/monitor/datasets?limit=${limit}`),

  getDataflows: (limit = 2000) =>
    apiGet<{ total: number; dataflows: DataflowRow[] }>(`/api/monitor/dataflows?limit=${limit}`),

  getProviderTypes: () =>
    apiGet<ProviderTypesResponse>('/api/monitor/provider-types'),

  getStatus: () =>
    apiGet<MonitorStatus>('/api/monitor/status'),

  triggerCheck: (staleHours: number, minCardCount: number, providerType: string) =>
    apiPost(`/api/monitor/check?stale_hours=${staleHours}&min_card_count=${minCardCount}&provider_type=${providerType}&max_workers=10`),

  crawlDatasets: () => apiPost('/api/monitor/crawl/datasets?max_workers=10'),
  crawlDataflows: () => apiPost('/api/monitor/crawl/dataflows?max_workers=10'),

  getAlerts: () => apiGet<AlertData>('/api/monitor/alerts'),

  getAutoCheckConfig: () => apiGet<AutoCheckConfig>('/api/monitor/auto-check-config'),
  saveAutoCheckConfig: (data: SaveConfigPayload) => apiPost('/api/monitor/save-alert-config', data),
  runAutoCheck: (data: AutoCheckPayload) => apiPost<JobStatusResponse>('/api/monitor/auto-check', data),

  exportDatasetsCsv: (params: string) => apiDownload(`/api/monitor/export/datasets/csv?${params}`),
  exportDataflowsCsv: (params: string) => apiDownload(`/api/monitor/export/dataflows/csv?${params}`),
}
