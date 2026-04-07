import { useState, useEffect, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { authService } from '../services/auth.service'
import {
  monitorService,
  type DatasetRow,
  type DataflowRow,
  type HealthCheckResult,
  type HealthAlert,
  type CrawlProgress,
  type MonitorStatus,
} from '../services/monitor.service'

type Tab = 'overview' | 'datasets' | 'dataflows'
const VALID_TABS: Tab[] = ['overview', 'datasets', 'dataflows']

// ─── Grouped State Interfaces ────────────────────────────

interface CrawlState {
  loading: boolean
  crawling: boolean
  crawlType: string        // '' | 'health' | 'datasets' | 'dataflows'
  result: HealthCheckResult | null
  error: string
  progress: CrawlProgress | null
}

interface Filters {
  staleHours: number
  minCardCount: number
  providerType: string
  dsSearch: string
  dfSearch: string
  dsFilterType: string
  dsFilterCardDir: string  // '' | 'gte' | 'lt'
  dsFilterCardVal: number
}

interface SortState<T extends string> {
  sortBy: T
  sortOrder: 'ASC' | 'DESC'
}

// ─── Hook ────────────────────────────────────────────────

export function useMonitor() {
  const [searchParams, setSearchParams] = useSearchParams()

  // Tab — persisted in URL
  const tab = (VALID_TABS.includes(searchParams.get('tab') as Tab)
    ? searchParams.get('tab') as Tab : 'overview')
  const setTab = (t: Tab) => setSearchParams({ tab: t }, { replace: true })

  // Domo instance URL
  const [domoBase, setDomoBase] = useState('')

  // Crawl state (grouped)
  const [crawl, setCrawl] = useState<CrawlState>({
    loading: false, crawling: false, crawlType: '',
    result: null, error: '', progress: null,
  })
  const updateCrawl = (patch: Partial<CrawlState>) =>
    setCrawl(prev => ({ ...prev, ...patch }))

  // Filters (grouped)
  const [filters, setFilters] = useState<Filters>({
    staleHours: 24, minCardCount: 0, providerType: '',
    dsSearch: '', dfSearch: '', dsFilterType: '',
    dsFilterCardDir: '', dsFilterCardVal: 40,
  })
  const updateFilters = (patch: Partial<Filters>) =>
    setFilters(prev => ({ ...prev, ...patch }))

  // Provider types dropdown
  const [providerTypes, setProviderTypes] = useState<string[]>([])

  // Data
  const [datasets, setDatasets] = useState<DatasetRow[]>([])
  const [dataflows, setDataflows] = useState<DataflowRow[]>([])
  const [dsTotal, setDsTotal] = useState(0)
  const [dfTotal, setDfTotal] = useState(0)

  // Sort
  const [dsSort, setDsSort] = useState<SortState<keyof DatasetRow>>({ sortBy: 'card_count', sortOrder: 'DESC' })
  const [dfSort, setDfSort] = useState<SortState<keyof DataflowRow>>({ sortBy: 'last_execution_time', sortOrder: 'DESC' })

  const handleDsSort = (field: keyof DatasetRow) => {
    setDsSort(prev =>
      prev.sortBy === field
        ? { ...prev, sortOrder: prev.sortOrder === 'ASC' ? 'DESC' : 'ASC' }
        : { sortBy: field, sortOrder: 'DESC' }
    )
  }
  const handleDfSort = (field: keyof DataflowRow) => {
    setDfSort(prev =>
      prev.sortBy === field
        ? { ...prev, sortOrder: prev.sortOrder === 'ASC' ? 'DESC' : 'ASC' }
        : { sortBy: field, sortOrder: 'DESC' }
    )
  }

  // ─── Data Loaders ──────────────────────────────────────

  const loadDatasets = useCallback(() => {
    monitorService.getDatasets()
      .then(d => { setDatasets(d.datasets || []); setDsTotal(d.total || 0) })
      .catch(() => {})
  }, [])

  const loadDataflows = useCallback(() => {
    monitorService.getDataflows()
      .then(d => { setDataflows(d.dataflows || []); setDfTotal(d.total || 0) })
      .catch(() => {})
  }, [])

  const loadProviderTypes = useCallback(() => {
    monitorService.getProviderTypes()
      .then(d => setProviderTypes(d.provider_types || []))
      .catch(() => {})
  }, [])

  // ─── Polling ───────────────────────────────────────────

  const startPolling = useCallback((onLoad?: () => void) => {
    const poll = setInterval(async () => {
      try {
        const status: MonitorStatus = await monitorService.getStatus()
        if (status.progress) updateCrawl({ progress: status.progress })
        if (status.status === 'completed') {
          if (status.result) updateCrawl({ result: status.result })
          updateCrawl({ loading: false, crawling: false, crawlType: '', progress: null })
          clearInterval(poll)
          loadDatasets(); loadDataflows(); loadProviderTypes()
          onLoad?.()
        }
      } catch { /* keep polling */ }
    }, 2000)
    setTimeout(() => {
      clearInterval(poll)
      updateCrawl({ loading: false, crawling: false, crawlType: '', progress: null })
    }, 300000)
  }, [loadDatasets, loadDataflows, loadProviderTypes])

  // ─── Actions ───────────────────────────────────────────

  const triggerCheck = async () => {
    updateCrawl({ loading: true, error: '', crawlType: 'health' })
    try {
      await monitorService.triggerCheck(filters.staleHours, filters.minCardCount, filters.providerType)
      updateCrawl({ crawling: true }); startPolling()
    } catch (err) {
      updateCrawl({ error: err instanceof Error ? err.message : 'Error', loading: false, crawlType: '' })
    }
  }

  const crawlDs = async () => {
    updateCrawl({ loading: true, error: '', crawlType: 'datasets' })
    try {
      await monitorService.crawlDatasets()
      updateCrawl({ crawling: true }); startPolling()
    } catch (err) {
      updateCrawl({ error: err instanceof Error ? err.message : 'Error', loading: false, crawlType: '' })
    }
  }

  const crawlDf = async () => {
    updateCrawl({ loading: true, error: '', crawlType: 'dataflows' })
    try {
      await monitorService.crawlDataflows()
      updateCrawl({ crawling: true }); startPolling()
    } catch (err) {
      updateCrawl({ error: err instanceof Error ? err.message : 'Error', loading: false, crawlType: '' })
    }
  }

  const checkStatus = useCallback(async () => {
    try {
      const status = await monitorService.getStatus()
      if (status.status === 'running') {
        updateCrawl({ crawling: true, loading: true })
        startPolling()
      }
      if (status.result) updateCrawl({ result: status.result })
    } catch { /* ignore */ }
  }, [startPolling])

  const refresh = () => { loadDatasets(); loadDataflows(); checkStatus() }

  // ─── Init ──────────────────────────────────────────────

  useEffect(() => {
    authService.getStatus()
      .then(d => { if (d.domo_url) setDomoBase(d.domo_url) })
      .catch(() => {})
    loadDatasets()
    loadDataflows()
    loadProviderTypes()
    checkStatus()
  }, [loadDatasets, loadDataflows, loadProviderTypes, checkStatus])

  return {
    // Tab
    tab, setTab,
    // Config
    domoBase,
    // Crawl state
    crawl, updateCrawl,
    // Filters
    filters, updateFilters,
    providerTypes,
    // Data
    datasets, dataflows, dsTotal, dfTotal,
    // Sort
    dsSort, dfSort, handleDsSort, handleDfSort,
    // Actions
    triggerCheck, crawlDs, crawlDf, refresh,
    loadDatasets, loadDataflows,
  }
}

export type { Tab, DatasetRow, DataflowRow, HealthCheckResult, HealthAlert, CrawlProgress }
