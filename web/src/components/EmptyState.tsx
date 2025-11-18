import { motion } from 'framer-motion'
import { Sparkles } from 'lucide-react'

export default function EmptyState() {
  return (
    <div className="h-full flex items-center justify-center text-center">
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        className="max-w-md space-y-4"
      >
        <div className="inline-flex h-16 w-16 rounded-full bg-white/10 items-center justify-center mb-2">
          <Sparkles className="w-7 h-7" />
        </div>
        <h2 className="text-3xl font-semibold">Selecciona un chat</h2>
        <p className="text-white/60">
          Toda la actividad, hilos y conversaciones en tiempo real aparecerán aquí. Elige un canal en la
          barra izquierda para comenzar.
        </p>
      </motion.div>
    </div>
  )
}


