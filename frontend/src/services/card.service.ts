import { apiGet } from '../api'

// ─── Types ───────────────────────────────────────────────

export interface Card {
  id: string
  title: string
  card_type: string
  view_count: number
  owner_name: string
  page_id: string | null
  page_title: string | null
  last_viewed_at: string | null
}

export interface DashboardRow {
  page_id: string
  page_title: string
  card_count: number
  total_views: number
}

export interface CardStats {
  total_cards: number
  total_dashboards: number
  total_views: number
  total_types: number
  zero_view_cards: number
  type_distribution: { card_type: string; count: number; views: number }[]
  top_dashboards: { page_title: string; card_count: number; total_views: number }[]
}

export interface LowUsageData {
  total: number
  max_views_threshold: number
  cards: Card[]
  by_owner: { owner_name: string; card_count: number; total_views: number; zero_view_count: number }[]
  by_dashboard: { page_title: string; page_id: string; card_count: number; total_views: number; zero_view_count: number }[]
  by_type: { card_type: string; card_count: number; total_views: number }[]
}

export interface PagedResponse<T> {
  data: T[]
  total: number
  page: number
  page_size: number
  total_pages: number
}

export interface LowUsageByDatasetRow {
  datasource_id: string
  datasource_name: string
  total_cards: number
  low_usage_count: number
  low_usage_pct: number
}

export interface LowUsageByDatasetResponse {
  by_dashboard: LowUsageByDatasetRow[]
  datasets: unknown[]
}

// ─── Service ─────────────────────────────────────────────

export const cardService = {
  getStats: () =>
    apiGet<CardStats>('/api/cards/stats'),

  getTypes: () =>
    apiGet<string[]>('/api/cards/types'),

  getList: (params: string) =>
    apiGet<PagedResponse<Card>>(`/api/cards/list?${params}`),

  getDashboards: (params: string) =>
    apiGet<PagedResponse<DashboardRow>>(`/api/cards/dashboards?${params}`),

  getLowUsage: (params: string) =>
    apiGet<LowUsageData>(`/api/cards/low-usage?${params}`),

  getLowUsageByDataset: (params: string) =>
    apiGet<LowUsageByDatasetResponse>(`/api/cards/low-usage-by-dataset?${params}`),
}
