import { motion } from 'framer-motion'

export default function TypingIndicator() {
  return (
    <div className="flex justify-start pl-2">
      <div className="bg-white/5 border border-white/10 rounded-2xl px-4 py-2 inline-flex items-center gap-2">
        <span className="text-xs text-white/60">Escribiendo</span>
        <div className="flex gap-1">
          {[0, 1, 2].map((i) => (
            <motion.span
              key={i}
              className="h-2 w-2 rounded-full bg-white/70"
              animate={{ opacity: [0.2, 1, 0.2], y: [-1, 2, -1] }}
              transition={{ duration: 0.9, repeat: Infinity, delay: i * 0.1 }}
            />
          ))}
        </div>
      </div>
    </div>
  )
}


