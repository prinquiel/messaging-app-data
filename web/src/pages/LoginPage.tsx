import { motion } from 'framer-motion'
import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Sparkles } from 'lucide-react'
import { useAuth } from '../auth/AuthContext'

export default function LoginPage() {
  const { login } = useAuth()
  const nav = useNavigate()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [err, setErr] = useState('')

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setErr('')
    try {
      await login(username, password)
      nav('/')
    } catch (e: any) {
      setErr(e?.response?.data?.detail || 'Login failed')
    }
  }

  return (
    <div className="min-h-screen bg-slate-950 text-white overflow-hidden relative px-6 py-10 flex items-center justify-center">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,_rgba(99,102,241,0.4),_transparent_45%)]" />
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        className="relative w-full max-w-lg glass-panel rounded-[32px] p-10 space-y-6"
      >
        <div className="flex items-center gap-3">
          <div className="h-12 w-12 rounded-2xl bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center">
            <Sparkles className="w-5 h-5" />
          </div>
          <div>
            <p className="text-xs uppercase tracking-[0.4em] text-white/60">Entrar</p>
            <h1 className="text-2xl font-semibold">Vortex Messenger</h1>
          </div>
        </div>

        <form onSubmit={onSubmit} className="space-y-4">
          {err && <div className="text-rose-300 text-sm">{err}</div>}
          <div>
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Usuario</label>
            <input
              className="glass-input w-full mt-2 p-3"
              placeholder="tu_usuario"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
            />
          </div>
          <div>
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Contraseña</label>
            <input
              className="glass-input w-full mt-2 p-3"
              placeholder="••••••••"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </div>
          <button className="w-full py-3 rounded-2xl bg-gradient-to-r from-brand-500 to-purple-600 font-semibold">
            Entrar
          </button>
        </form>

        <p className="text-sm text-white/60">
          ¿Sin cuenta?{' '}
          <Link to="/register" className="text-white font-semibold underline-offset-4 underline">
            Crear cuenta
          </Link>
        </p>
      </motion.div>
    </div>
  )
}


