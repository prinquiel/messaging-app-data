import { motion } from 'framer-motion'
import { MessageCircle, Sparkles } from 'lucide-react'
import { useNavigate, useLocation } from 'react-router-dom'
import { useChatStore } from '../store/chatStore'
import ChatAvatar from './ChatAvatar'

type Props = {
  isLoading?: boolean
}

export default function Sidebar({ isLoading }: Props) {
  const { chats, activeChatId, setActiveChat } = useChatStore()
  const navigate = useNavigate()
  const location = useLocation()

  const handleSelect = (chatId: number) => {
    setActiveChat(chatId)
    navigate(`/chats/${chatId}`)
  }

  return (
    <aside className="w-[320px] hidden md:flex flex-col gap-5 pl-4 py-4">
      <div className="glass-panel flex-1 rounded-3xl overflow-hidden flex flex-col">
        <div className="flex items-center justify-between px-5 pt-5 pb-3 border-b border-white/5">
          <div>
            <p className="text-xs text-white/50 uppercase tracking-[0.4em]">Inboxes</p>
            <h3 className="text-lg font-semibold">Chats</h3>
          </div>
          <span className="text-xs text-white/60">{chats.length}</span>
        </div>

        <div className="flex-1 overflow-y-auto scroll-area px-3 py-4 space-y-2">
          {isLoading && (
            <div className="text-center text-white/60 text-sm py-6">Cargando canales...</div>
          )}
          {!isLoading && chats.length === 0 && (
            <div className="flex flex-col items-center gap-2 py-8 text-center text-white/60 text-sm">
              <MessageCircle className="w-5 h-5" />
              Sin chats aún.
            </div>
          )}
          {chats.map((chat) => {
            const active = activeChatId === chat.id || location.pathname === `/chats/${chat.id}`
            return (
              <motion.button
                key={chat.id}
                onClick={() => handleSelect(chat.id)}
                whileHover={{ scale: 1.01 }}
                className={`w-full flex items-center gap-3 rounded-2xl px-3 py-3 transition ${
                  active
                    ? 'bg-gradient-to-r from-brand-600/70 to-purple-600/40 border border-white/20'
                    : 'hover:bg-white/5 border border-transparent'
                }`}
              >
                <ChatAvatar id={chat.id} name={chat.name} active={active} />
                <div className="flex-1 text-left">
                  <p className="font-semibold text-sm truncate">{chat.name || `Chat #${chat.id}`}</p>
                  <p className="text-xs text-white/60 truncate">{chat.last_message || 'Canal activo'}</p>
                </div>
                {chat.unread ? (
                  <span className="min-w-[24px] h-6 rounded-full bg-rose-500/80 px-2 text-xs font-semibold flex items-center justify-center">
                    {chat.unread}
                  </span>
                ) : (
                  <span className="h-2 w-2 rounded-full bg-emerald-400" />
                )}
              </motion.button>
            )
          })}
        </div>
      </div>
    </aside>
  )
}


