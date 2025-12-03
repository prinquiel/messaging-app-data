import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import axios from 'axios'
import { clearAnalyticsUser, setAnalyticsUser, trackEvent } from '../lib/analytics'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'
const USER_STORAGE_KEY = 'current_user'
const TOKEN_STORAGE_KEY = 'token'

const storage =
  typeof window !== 'undefined'
    ? (() => {
        try {
          return window.sessionStorage
        } catch {
          return undefined
        }
      })()
    : undefined

type AuthUser = {
  id: number
  username: string
  full_name: string
  email: string
  avatar_url?: string | null
}

type AuthContextType = {
  token: string | null
  userId?: number
  user?: AuthUser
  login: (username: string, password: string) => Promise<void>
  register: (p: { username: string, email: string, full_name: string, password: string }) => Promise<void>
  logout: () => void
  refreshUser: () => Promise<void>
}

const AuthContext = createContext<AuthContextType>({} as any)

type StoredUserPayload = {
  token: string
  user: AuthUser
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [token, setToken] = useState<string | null>(() => storage?.getItem(TOKEN_STORAGE_KEY) ?? null)
  const [userId, setUserId] = useState<number | undefined>(() => {
    const existing = storage?.getItem(TOKEN_STORAGE_KEY)
    if (!existing) return undefined
    return decodeUserId(existing)
  })
  const [user, setUser] = useState<AuthUser | undefined>(() => {
    try {
      const stored = storage?.getItem(USER_STORAGE_KEY)
      if (!stored) return undefined
      const parsed = JSON.parse(stored) as StoredUserPayload
      if (!parsed?.token || !parsed?.user) return undefined
      const activeToken = storage?.getItem(TOKEN_STORAGE_KEY)
      if (!activeToken || parsed.token !== activeToken) return undefined
      return parsed.user
    } catch {
      return undefined
    }
  })

  useEffect(() => {
    if (!storage) return
    if (user && token) {
      const payload: StoredUserPayload = { token, user }
      storage.setItem(USER_STORAGE_KEY, JSON.stringify(payload))
    } else {
      storage?.removeItem(USER_STORAGE_KEY)
    }
  }, [user, token])

  useEffect(() => {
    if (!storage) return
    if (token) storage.setItem(TOKEN_STORAGE_KEY, token)
    else storage.removeItem(TOKEN_STORAGE_KEY)
    setUserId(token ? decodeUserId(token) : undefined)
    if (!token) {
      setUser(undefined)
    }
  }, [token])

  useEffect(() => {
    if (!token) {
      setUser(undefined)
      return
    }
    fetchUserProfile(token)
  }, [token])

  useEffect(() => {
    if (user) {
      void setAnalyticsUser({
        id: user.id,
        email: user.email,
        username: user.username,
        fullName: user.full_name,
      })
    } else {
      void clearAnalyticsUser()
    }
  }, [user])

  async function fetchUserProfile(accessToken: string) {
    const id = decodeUserId(accessToken)
    if (!id) return
    try {
      const { data } = await axios.get(`${API_URL}/users/${id}`, {
        headers: { Authorization: `Bearer ${accessToken}` },
      })
      setUser(data)
    } catch (error) {
      console.error('No se pudo obtener el usuario autenticado', error)
    }
  }

  const refreshUser = async () => {
    if (token) {
      await fetchUserProfile(token)
    }
  }

  const login = async (username: string, password: string) => {
    const { data } = await axios.post(`${API_URL}/auth/login`, { username, password })
    setToken(data.access_token)
    void trackEvent('login_success', { method: 'credentials' })
  }

  const register = async (p: { username: string, email: string, full_name: string, password: string }) => {
    await axios.post(`${API_URL}/auth/register`, p)
    void trackEvent('register_success', { method: 'credentials' })
    await login(p.username, p.password)
  }

  const logout = () => {
    void trackEvent('logout')
    void clearAnalyticsUser()
    setToken(null)
    setUser(undefined)
  }

  const value = useMemo(
    () => ({ token, userId, user, login, register, logout, refreshUser }),
    [token, userId, user],
  )
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  return useContext(AuthContext)
}

function decodeUserId(token: string): number | undefined {
  try {
    const payload = JSON.parse(atob(token.split('.')[1]))
    return payload?.user_id
  } catch {
    return undefined
  }
}


