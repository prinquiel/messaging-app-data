import { motion } from 'framer-motion'
import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Sparkles } from 'lucide-react'
import { useAuth } from '../auth/AuthContext'

export default function RegisterPage() {
  const { register } = useAuth()
  const nav = useNavigate()
  const [form, setForm] = useState({ username: '', email: '', full_name: '', password: '' })
  const [err, setErr] = useState('')

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setErr('')
    try {
      await register(form)
      nav('/')
    } catch (e: any) {
      setErr(e?.response?.data?.detail || 'Register failed')
    }
  }

  return (
    <div className="min-h-screen bg-slate-950 text-white overflow-hidden relative px-6 py-10 flex items-center justify-center">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_bottom,_rgba(56,189,248,0.25),_transparent_45%)]" />
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        className="relative w-full max-w-2xl glass-panel rounded-[32px] p-10 space-y-6"
      >
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="flex items-center gap-3">
            <div className="h-12 w-12 rounded-2xl bg-gradient-to-br from-sky-500 to-blue-600 flex items-center justify-center">
              <Sparkles className="w-5 h-5" />
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.4em] text-white/60">Crear cuenta</p>
              <h1 className="text-2xl font-semibold">Explora Vortex</h1>
            </div>
          </div>
          <Link to="/login" className="text-sm text-white/60 underline">
            Ya tengo cuenta
          </Link>
        </div>

        <form onSubmit={onSubmit} className="grid grid-cols-1 md:grid-cols-2 gap-5">
          {err && <div className="text-rose-300 text-sm col-span-full">{err}</div>}
          <div className="col-span-1">
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Usuario</label>
            <input
              className="glass-input w-full mt-2 p-3"
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
            />
          </div>
          <div className="col-span-1">
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Nombre completo</label>
            <input
              className="glass-input w-full mt-2 p-3"
              value={form.full_name}
              onChange={(e) => setForm({ ...form, full_name: e.target.value })}
            />
          </div>
          <div className="col-span-1">
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Email</label>
            <input
              className="glass-input w-full mt-2 p-3"
              type="email"
              value={form.email}
              onChange={(e) => setForm({ ...form, email: e.target.value })}
            />
          </div>
          <div className="col-span-1">
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Contraseña</label>
            <input
              className="glass-input w-full mt-2 p-3"
              type="password"
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
            />
          </div>
          <button className="col-span-full py-3 rounded-2xl bg-gradient-to-r from-sky-500 to-indigo-500 font-semibold">
            Crear cuenta
          </button>
        </form>
      </motion.div>
    </div>
  )
}


