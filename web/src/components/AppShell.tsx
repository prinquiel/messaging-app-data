import { useEffect, useState } from 'react'
import { Outlet, useLocation, useNavigate, Link } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { MessageCirclePlus, Plus, Search, ShoppingBag } from 'lucide-react'
import { api, authHeaders } from '../lib/api'
import { useAuth } from '../auth/AuthContext'
import { useChatStore } from '../store/chatStore'
import Sidebar from './Sidebar'
import ThemeToggle from './ThemeToggle'
import LanguageToggle from './LanguageToggle'

const INTERNAL_MARKETPLACE_DESCRIPTION = '__marketplace_seller__'

export default function AppShell() {
  const { token, logout, user } = useAuth()
  const { chats, setChats } = useChatStore()
  const [isFetching, setIsFetching] = useState(false)
  const location = useLocation()
  const navigate = useNavigate()

  useEffect(() => {
    if (!token || chats.length) return
    setIsFetching(true)
    api
      .get('/me/chats', { headers: authHeaders(token) })
      .then(({ data }) => {
        const items = Array.isArray(data) ? data : data.items || []
        const enhanced = items
          .filter((chat: any) => chat.description !== INTERNAL_MARKETPLACE_DESCRIPTION)
          .map((chat: any) => ({
            ...chat,
            unread: chat.id % 4 === 0 ? Math.floor(Math.random() * 5) + 1 : 0,
            last_message: chat.description ?? '¡Charla lista!',
          }))
        setChats(enhanced)
      })
      .finally(() => setIsFetching(false))
  }, [token, chats.length, setChats])

  return (
    <div className="min-h-screen w-full text-slate-100 bg-slate-950/95">
      <div className="flex h-screen gap-4 bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
        <Sidebar isLoading={isFetching} />

        <div className="flex-1 mr-4 my-4 glass-panel rounded-3xl flex flex-col overflow-hidden">
          <header className="flex items-center justify-between px-8 py-6 border-b border-white/5">
            <div>
              <p className="text-sm uppercase tracking-[0.2em] text-white/60">Messaging Workspace</p>
              <h1 className="text-2xl font-semibold text-white">Conversaciones</h1>
            </div>
            <div className="flex items-center gap-3">
              <button className="glass-panel p-2 rounded-2xl hover:bg-white/10 transition">
                <Search className="w-5 h-5" />
              </button>
              <Link
                to="/marketplace"
                className="glass-panel px-3 py-2 rounded-2xl hover:bg-white/10 transition flex items-center gap-2"
              >
                <span className="text-sm font-semibold">Marketplace</span>
              </Link>
              <button
                onClick={() => navigate('/')}
                className="glass-panel px-3 py-2 rounded-2xl hover:bg-white/10 transition flex items-center gap-2"
              >
                <MessageCirclePlus className="w-5 h-5" />
                <span className="text-sm font-semibold">Crear chat</span>
              </button>
              <button
                onClick={() => navigate('/')}
                className={`hidden sm:flex items-center gap-3 px-3 py-2 rounded-2xl bg-white/5 border border-white/10 hover:bg-white/10 transition`}
              >
                <div className="relative">
                  <img
                    src={`https://api.dicebear.com/7.x/bottts/svg?seed=${token?.slice(0, 8)}`}
                    className="h-10 w-10 rounded-full"
                    alt="avatar"
                  />
                  <span className="absolute bottom-0 right-0 h-3 w-3 rounded-full bg-emerald-400 border-2 border-slate-900" />
                </div>
                <div className="text-left">
                  <p className="font-semibold leading-tight">
                    {user?.full_name || user?.username || 'Tu cuenta'}
                  </p>
                  <p className="text-xs text-white/60">Disponible</p>
                </div>
              </button>
              <LanguageToggle />
              <ThemeToggle />
              <button
                onClick={logout}
                className="px-3 py-2 rounded-2xl bg-rose-500/80 hover:bg-rose-500 transition text-sm font-semibold"
              >
                Salir
              </button>
            </div>
          </header>

          <div className="flex-1 overflow-hidden relative">
            <AnimatePresence mode="wait">
              <motion.div
                key={location.pathname}
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -12 }}
                transition={{ duration: 0.25 }}
                className="h-full"
              >
                <Outlet />
              </motion.div>
            </AnimatePresence>
          </div>
        </div>
      </div>
    </div>
  )
}


