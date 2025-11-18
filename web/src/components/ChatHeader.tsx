import { Phone, Search, UsersRound } from 'lucide-react'
import ChatAvatar from './ChatAvatar'

type Props = {
  title: string
  subtitle?: string
  chatId: number
  presenceLabel?: string
}

export default function ChatHeader({ title, subtitle, chatId, presenceLabel }: Props) {
  return (
    <div className="flex items-center justify-between px-3 py-4 border-b border-white/5">
      <div className="flex items-center gap-4">
        <ChatAvatar id={chatId} name={title} />
        <div>
          <h2 className="text-xl font-semibold">{title}</h2>
          <div className="flex items-center gap-2 text-sm text-white/60">
            <span>{subtitle || 'Activo · 12 participantes'}</span>
            {presenceLabel && (
              <span className="flex items-center gap-1 text-xs px-2 py-0.5 rounded-full bg-white/10">
                <span className="h-2 w-2 rounded-full bg-emerald-400" />
                {presenceLabel}
              </span>
            )}
          </div>
        </div>
      </div>
      <div className="flex items-center gap-2">
        <button className="glass-panel p-2.5 rounded-2xl hover:bg-white/10 transition">
          <Search className="w-5 h-5" />
        </button>
        <button className="glass-panel p-2.5 rounded-2xl hover:bg-white/10 transition">
          <UsersRound className="w-5 h-5" />
        </button>
        <button className="glass-panel p-2.5 rounded-2xl hover:bg-white/10 transition">
          <Phone className="w-5 h-5" />
        </button>
      </div>
    </div>
  )
}


