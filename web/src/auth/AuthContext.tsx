import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import axios from 'axios'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

type AuthContextType = {
  token: string | null
  userId?: number
  login: (username: string, password: string) => Promise<void>
  register: (p: { username: string, email: string, full_name: string, password: string }) => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthContextType>({} as any)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem('token'))
  const [userId, setUserId] = useState<number | undefined>(() => {
    const existing = localStorage.getItem('token')
    if (!existing) return undefined
    return decodeUserId(existing)
  })
  useEffect(() => {
    if (token) localStorage.setItem('token', token)
    else localStorage.removeItem('token')
    setUserId(token ? decodeUserId(token) : undefined)
  }, [token])

  const login = async (username: string, password: string) => {
    const { data } = await axios.post(`${API_URL}/auth/login`, { username, password })
    setToken(data.access_token)
  }

  const register = async (p: { username: string, email: string, full_name: string, password: string }) => {
    await axios.post(`${API_URL}/auth/register`, p)
    await login(p.username, p.password)
  }

  const logout = () => setToken(null)

  const value = useMemo(() => ({ token, userId, login, register, logout }), [token, userId])
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


