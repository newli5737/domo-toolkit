/**
 * BeastMode Service — API calls for BeastMode module.
 * Types: page-level types live in components/BeastMode/types.ts (single source of truth).
 * This service uses generics so callers provide their own types.
 */
import { apiGet, apiPost, apiDelete, apiDownload } from '../api'
import type { Summary, BmRow } from '../components/BeastMode/types'

export const beastmodeService = {
  // ─── Crawl actions ─────────────────────────────────
  startCrawl: () =>
    apiPost<{ job_id: number; message: string }>('/api/beastmode/crawl'),

  reanalyze: (threshold: number) =>
    apiPost<{ job_id: number; message: string }>('/api/beastmode/crawl/reanalyze', { low_view_threshold: threshold }),

  cancelCrawl: () =>
    apiPost('/api/beastmode/crawl/cancel'),

  retryDetails: () =>
    apiPost<{ job_id: number; message: string }>('/api/beastmode/crawl/retry-details'),

  bmOnlyCrawl: () =>
    apiPost<{ job_id: number; message: string }>('/api/beastmode/crawl/bm-only'),

  // ─── Read-only ─────────────────────────────────────
  getSummary: () =>
    apiGet<Summary>('/api/beastmode/summary'),

  getGroup: (group: number, limit = 5000) =>
    apiGet<{ data: BmRow[] }>(`/api/beastmode/group/${group}?limit=${limit}`),

  search: (query: string, limit = 50) =>
    apiGet<{ data: BmRow[] }>(`/api/beastmode/search?q=${encodeURIComponent(query)}&limit=${limit}`),

  deleteBm: (bmId: number) =>
    apiDelete(`/api/beastmode/${bmId}`),

  exportCsv: (lang: string, group: number) =>
    apiDownload(`/api/beastmode/export/csv?lang=${lang}&group=${group}`),
}
