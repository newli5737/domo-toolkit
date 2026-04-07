import { useEffect, useState } from 'react'
import {
  BarChart3,
  Database,
  GitBranch,
  Zap,
  TrendingUp,
  Clock,
  Activity,
} from 'lucide-react'
import { beastmodeService } from '../services/beastmode.service'
import { monitorService } from '../services/monitor.service'
import { useI18n } from '../i18n'


interface Stats {
  bm_total: number
  bm_groups: number
  dataset_total: number
  dataflow_total: number
}

export default function Dashboard() {
  const { t } = useI18n()
  const [stats, setStats] = useState<Stats>({
    bm_total: 0, bm_groups: 0, dataset_total: 0, dataflow_total: 0,
  })

  useEffect(() => {
    beastmodeService.getSummary()
      .then(d => setStats(prev => ({
        ...prev,
        bm_total: d.total || 0,
        bm_groups: Array.isArray(d.groups) ? d.groups.length : 0,
      })))
      .catch(() => {})

    monitorService.getDatasets(1)
      .then(d => setStats(prev => ({ ...prev, dataset_total: d.total || 0 })))
      .catch(() => {})

    monitorService.getDataflows(1)
      .then(d => setStats(prev => ({ ...prev, dataflow_total: d.total || 0 })))
      .catch(() => {})
  }, [])

  return (
    <div className="animate-fadein">
      <div className="page-header">
        <h1 className="flex items-center gap-2">
          <BarChart3 className="w-6 h-6 text-blue-500" />
          {t('dashboard.title')}
        </h1>
        <p>{t('dashboard.desc')}</p>
      </div>

      <div className="page-body">
        <div className="grid grid-cols-4 gap-4 mb-8">
          <div className="stat-card">
            <div className="stat-icon bg-violet-50"><Zap className="w-5 h-5 text-violet-500" /></div>
            <div>
              <div className="stat-value">{stats.bm_total || '-'}</div>
              <div className="stat-label">{t('dashboard.beastmodes')}</div>
            </div>
          </div>
          <div className="stat-card">
            <div className="stat-icon bg-blue-50"><Database className="w-5 h-5 text-blue-500" /></div>
            <div>
              <div className="stat-value">{stats.dataset_total || '-'}</div>
              <div className="stat-label">{t('dashboard.datasets')}</div>
            </div>
          </div>
          <div className="stat-card">
            <div className="stat-icon bg-cyan-50"><GitBranch className="w-5 h-5 text-cyan-500" /></div>
            <div>
              <div className="stat-value">{stats.dataflow_total || '-'}</div>
              <div className="stat-label">{t('dashboard.dataflows')}</div>
            </div>
          </div>
          <div className="stat-card">
            <div className="stat-icon bg-green-50"><TrendingUp className="w-5 h-5 text-green-500" /></div>
            <div>
              <div className="stat-value">{stats.bm_groups || '-'}</div>
              <div className="stat-label">{t('dashboard.bmGroups')}</div>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-header">{t('dashboard.quickActions')}</div>
          <div className="card-body">
            <div className="grid grid-cols-3 gap-4">
              <a href="/beastmode" className="flex items-center gap-3 p-4 rounded-lg border border-slate-200 hover:border-blue-300 hover:bg-blue-50/50 transition-all no-underline text-slate-700">
                <Zap className="w-5 h-5 text-violet-500" />
                <div>
                  <div className="text-sm font-semibold">{t('dashboard.bmCleanup')}</div>
                  <div className="text-xs text-slate-400">{t('dashboard.bmCleanupDesc')}</div>
                </div>
              </a>
              <a href="/monitor" className="flex items-center gap-3 p-4 rounded-lg border border-slate-200 hover:border-blue-300 hover:bg-blue-50/50 transition-all no-underline text-slate-700">
                <Activity className="w-5 h-5 text-blue-500" />
                <div>
                  <div className="text-sm font-semibold">{t('dashboard.monitor')}</div>
                  <div className="text-xs text-slate-400">{t('dashboard.monitorDesc')}</div>
                </div>
              </a>
              <a href="/login" className="flex items-center gap-3 p-4 rounded-lg border border-slate-200 hover:border-blue-300 hover:bg-blue-50/50 transition-all no-underline text-slate-700">
                <Clock className="w-5 h-5 text-orange-500" />
                <div>
                  <div className="text-sm font-semibold">{t('dashboard.session')}</div>
                  <div className="text-xs text-slate-400">{t('dashboard.sessionDesc')}</div>
                </div>
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
