const API_BASE = import.meta.env.VITE_API_BASE || '';

export async function apiPost<T = unknown>(path: string, body?: unknown): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(err.detail || 'Request failed');
  }
  return resp.json();
}

export async function apiGet<T = unknown>(path: string): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`);
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(err.detail || 'Request failed');
  }
  return resp.json();
}

export async function apiDelete<T = unknown>(path: string): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, { method: 'DELETE' });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(err.detail || 'Request failed');
  }
  return resp.json();
}

export function apiDownload(path: string) {
  window.open(`${API_BASE}${path}`, '_blank');
}
