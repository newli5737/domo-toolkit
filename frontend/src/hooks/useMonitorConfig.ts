import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { monitorService } from '../services/monitor.service'
import type { SaveConfigPayload, AutoCheckPayload, JobStatusResponse } from '../services/monitor.service'
import { useI18n } from '../i18n'

export function useMonitorConfig() {
  const { lang } = useI18n()
  const queryClient = useQueryClient()

  // Form State
  const [form, setForm] = useState({
    minCards: 40,
    providerType: 'mysql-ssh',
    alertEmail: '',
    scheduleEnabled: false,
    scheduleHour: 8,
    scheduleMinute: 0,
    scheduleDays: 'mon,tue,wed,thu,fri'
  })

  const updateForm = (updates: Partial<typeof form>) => {
    setForm(prev => ({ ...prev, ...updates }))
  }
  
  const [runResult, setRunResult] = useState<Partial<JobStatusResponse> & { error?: string } | null>(null)

  // Fetch Config
  const { data: config, isLoading: isConfigLoading, error: configError } = useQuery({
    queryKey: ['monitor', 'config'],
    queryFn: monitorService.getAutoCheckConfig
  })

  // Fetch provider types
  const { data: providerTypesData } = useQuery({
    queryKey: ['monitor', 'providerTypes'],
    queryFn: monitorService.getProviderTypes,
    staleTime: 5 * 60 * 1000, // 5 min
  })

  // Sync config with local form state when loaded
  useEffect(() => {
    if (config) {
      updateForm({
        alertEmail: config.alert_email_to ?? '',
        minCards: config.min_card_count ?? 40,
        providerType: config.provider_type ?? '',
        scheduleEnabled: config.schedule_enabled ?? false,
        scheduleHour: config.schedule_hour ?? 8,
        scheduleMinute: config.schedule_minute ?? 0,
        scheduleDays: config.schedule_days ?? 'mon,tue,wed,thu,fri'
      })
    }
  }, [config])

  // Save config mutation
  const saveMutation = useMutation({
    mutationFn: (data: SaveConfigPayload) => monitorService.saveAutoCheckConfig(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['monitor', 'config'] })
    },
    onError: (error) => {
      console.error("Failed to save config:", error)
      alert(lang === 'vi' ? 'Lưu cấu hình thất bại!' : '設定の保存に失敗しました！')
    }
  })

  // Run auto check mutation
  const runMutation = useMutation({
    mutationFn: (data: AutoCheckPayload) => monitorService.runAutoCheck(data),
    onSuccess: (res) => {
      setRunResult(res)
    },
    onError: (error) => {
      setRunResult({ error: error instanceof Error ? error.message : 'Error' })
    }
  })

  const saveConfig = () => {
    saveMutation.mutate({
      alert_email: form.alertEmail,
      min_card_count: form.minCards,
      provider_type: form.providerType,
      schedule_enabled: form.scheduleEnabled,
      schedule_hour: form.scheduleHour,
      schedule_minute: form.scheduleMinute,
      schedule_days: form.scheduleDays,
    })
  }

  const runAutoCheck = () => {
    setRunResult(null)
    runMutation.mutate({
      min_card_count: form.minCards,
      provider_type: form.providerType,
      alert_email: form.alertEmail,
    })
  }

  return {
    config,
    isConfigLoading,
    configError,
    form,
    updateForm,
    providerTypes: providerTypesData?.provider_types || [],
    
    saveConfig,
    isSaving: saveMutation.isPending,
    isSaved: saveMutation.isSuccess,
    
    runAutoCheck,
    isRunning: runMutation.isPending,
    runResult
  }
}
