export const GROUP_CONFIG_VI = [
  { num: 1, label: 'Không sử dụng', color: 'red', icon: '🗑️' },
  { num: 2, label: 'Từng được dùng', color: 'orange', icon: '⏸️' },
  { num: 3, label: 'Card ít xem', color: 'cyan', icon: '👁️' },
  { num: 4, label: 'Đang hoạt động', color: 'green', icon: '✅' },
]

export const GROUP_CONFIG_JA = [
  { num: 1, label: '未使用', color: 'red', icon: '🗑️' },
  { num: 2, label: '過去使用', color: 'orange', icon: '⏸️' },
  { num: 3, label: '低閲覧', color: 'cyan', icon: '👁️' },
  { num: 4, label: '稼働中', color: 'green', icon: '✅' },
]

export const COLOR_MAP: Record<string, string> = {
  red: 'from-[var(--color-accent-red)] to-[var(--color-accent-orange)]',
  orange: 'from-[var(--color-accent-orange)] to-[var(--color-accent-yellow)]',
  cyan: 'from-[var(--color-accent-cyan)] to-[var(--color-accent-green)]',
  green: 'from-[var(--color-accent-green)] to-[var(--color-accent-cyan)]',
  blue: 'from-[var(--color-accent-blue)] to-[var(--color-accent-cyan)]',
  purple: 'from-[var(--color-accent-purple)] to-[var(--color-accent-blue)]',
  yellow: 'from-[var(--color-accent-yellow)] to-[var(--color-accent-orange)]',
}

export const TEXT_COLOR: Record<string, string> = {
  red: 'text-[var(--color-accent-red)]',
  orange: 'text-[var(--color-accent-orange)]',
  cyan: 'text-[var(--color-accent-cyan)]',
  green: 'text-[var(--color-accent-green)]',
  blue: 'text-[var(--color-accent-blue)]',
  purple: 'text-[var(--color-accent-purple)]',
  yellow: 'text-[var(--color-accent-yellow)]',
}

export const BADGE_BG: Record<string, string> = {
  red: 'bg-[var(--color-accent-red)]/15 text-[var(--color-accent-red)]',
  orange: 'bg-[var(--color-accent-orange)]/15 text-[var(--color-accent-orange)]',
  cyan: 'bg-[var(--color-accent-cyan)]/15 text-[var(--color-accent-cyan)]',
  green: 'bg-[var(--color-accent-green)]/15 text-[var(--color-accent-green)]',
}

export const STEP_ICONS = ['🔍', '📋', '🃏', '👁️', '📊']

export function formatTime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  const m = Math.floor(seconds / 60)
  const s = seconds % 60
  return `${m}m ${s}s`
}

export function estimateRemaining(elapsed: number, percent: number): string {
  if (percent <= 0 || elapsed <= 0) return '—'
  const totalEstimate = (elapsed / percent) * 100
  const remaining = Math.max(0, Math.round(totalEstimate - elapsed))
  if (remaining === 0) return '—'
  return `~${formatTime(remaining)}`
}
