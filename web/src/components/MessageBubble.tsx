import { motion } from 'framer-motion'

type Props = {
  content: string
  senderName?: string
  timestamp?: string
  isOwn?: boolean
  onSell?: () => void
}

export default function MessageBubble({ content, senderName, timestamp, isOwn, onSell }: Props) {
  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 8, scale: 0.98 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      className={`flex ${isOwn ? 'justify-end' : 'justify-start'}`}
    >
      <div className="relative">
        <div
          className={`max-w-[65%] rounded-2xl px-4 py-3 shadow-lg transition ${
            isOwn
              ? 'bg-gradient-to-br from-brand-500 to-purple-500 text-white rounded-br-sm'
              : 'bg-white/5 border border-white/10 rounded-bl-sm'
          }`}
        >
          {!isOwn && (
            <p className="text-xs text-white/60 font-semibold mb-1">{senderName || 'Usuario'}</p>
          )}
          <p className="text-sm leading-relaxed whitespace-pre-wrap">{content}</p>
          {timestamp && (
            <p className="text-[10px] text-white/50 mt-2 text-right">
              {new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </p>
          )}
        </div>
        {isOwn && onSell && (
          <button
            onClick={onSell}
            className="absolute -top-3 right-0 text-[10px] uppercase tracking-[0.3em] bg-white/10 text-white px-2 py-1 rounded-full backdrop-blur hover:bg-white/20 transition"
          >
            Vender
          </button>
        )}
      </div>
    </motion.div>
  )
}


