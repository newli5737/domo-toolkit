/**
 * Auth Service — Tập trung mọi API call liên quan authentication.
 */
import { apiGet, apiPost } from '../api'

export interface LoginResponse {
  success: boolean
  message: string
  username: string
}

export interface AuthStatus {
  logged_in: boolean
  username: string
  domo_url: string
}

export const authService = {
  getStatus: () => apiGet<AuthStatus>('/api/auth/status'),

  login: (username: string, password: string) =>
    apiPost<LoginResponse>('/api/auth/login', { username, password }),

  uploadCookies: (cookieData: unknown) =>
    apiPost<LoginResponse>('/api/auth/upload-cookies', cookieData),

  logout: () => apiPost<{ success: boolean; message: string }>('/api/auth/logout'),
}
