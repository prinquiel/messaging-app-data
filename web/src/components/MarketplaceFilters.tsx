import { useState } from 'react'
import { motion } from 'framer-motion'

export type MarketplaceFilters = {
  search: string
  categoryId: string
  chatId: string
}

type Category = {
  id: number
  name: string
}

type Chat = {
  id: number
  name?: string | null
}

type Props = {
  filters: MarketplaceFilters
  onChange: (next: MarketplaceFilters) => void
  categories: Category[]
  chats: Chat[]
}

export default function MarketplaceFilters({ filters, onChange, categories, chats }: Props) {
  const [expanded, setExpanded] = useState(true)
  const handleChange = (key: keyof MarketplaceFilters, value: string) => {
    onChange({ ...filters, [key]: value })
  }

  return (
    <motion.div
      layout
      className="glass-panel rounded-3xl p-5 space-y-4 border border-white/10"
    >
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs uppercase tracking-[0.4em] text-white/60">Marketplace</p>
          <h2 className="text-xl font-semibold">Explorar productos</h2>
        </div>
        <button
          onClick={() => setExpanded((v) => !v)}
          className="text-xs text-white/60 underline underline-offset-4"
        >
          {expanded ? 'Ocultar filtros' : 'Mostrar filtros'}
        </button>
      </div>
      {expanded && (
        <motion.div layout className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="md:col-span-2 space-y-2">
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Buscar</label>
            <input
              className="glass-input w-full p-3"
              placeholder="Título, descripción…"
              value={filters.search}
              onChange={(e) => handleChange('search', e.target.value)}
            />
          </div>
          <div className="space-y-2">
            <label htmlFor="category-filter" className="text-xs uppercase tracking-[0.4em] text-white/50">
              Categoría
            </label>
            <select
              id="category-filter"
              className="glass-input w-full p-3 bg-transparent"
              value={filters.categoryId}
              onChange={(e) => handleChange('categoryId', e.target.value)}
            >
              <option value="">Todas</option>
              {categories.map((category) => (
                <option key={category.id} value={String(category.id)}>
                  {category.name}
                </option>
              ))}
            </select>
          </div>
          <div className="space-y-2 md:col-span-2">
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Chat</label>
            <select
              className="glass-input w-full p-3 bg-transparent"
              value={filters.chatId}
              onChange={(e) => handleChange('chatId', e.target.value)}
            >
              <option value="">Todos</option>
              {chats.map((chat) => (
                <option key={chat.id} value={chat.id}>
                  {chat.name || `Chat #${chat.id}`}
                </option>
              ))}
            </select>
          </div>
        </motion.div>
      )}
    </motion.div>
  )
}


