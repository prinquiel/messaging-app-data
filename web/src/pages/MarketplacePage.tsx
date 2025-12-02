import { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { useAuth } from '../auth/AuthContext'
import { api, authHeaders } from '../lib/api'
import MarketplaceCard, { MarketplaceItem } from '../components/MarketplaceCard'
import MarketplaceFilters, { MarketplaceFilters as FilterState } from '../components/MarketplaceFilters'
import { useChatStore } from '../store/chatStore'
import SellItemModal from '../components/ListingModal'

type Category = {
  id: number
  name: string
}

export default function MarketplacePage() {
  const { token } = useAuth()
  const { chats } = useChatStore()
  const navigate = useNavigate()

  const [categories, setCategories] = useState<Category[]>([])
  const [items, setItems] = useState<MarketplaceItem[]>([])
  const [loading, setLoading] = useState(false)
  const [showModal, setShowModal] = useState(false)
  const [refreshKey, setRefreshKey] = useState(0)
  const [filters, setFilters] = useState<FilterState>({
    search: '',
    categoryId: '',
    chatId: '',
  })

  useEffect(() => {
    const loadCategories = async () => {
      try {
        const { data } = await api.get('/categories?page=1&page_size=100', {
          headers: authHeaders(token),
        })
        const list = Array.isArray(data.items) ? data.items : data
        setCategories(list)
      } catch (error) {
        console.error('No se pudieron cargar categorías', error)
      }
    }
    loadCategories()
  }, [token])

  useEffect(() => {
    const params = new URLSearchParams({
      page: '1',
      page_size: '24',
    })
    if (filters.search) params.set('search', filters.search)
    params.set('status', 'active')
    if (filters.categoryId) params.set('category_id', filters.categoryId)
    if (filters.chatId) params.set('chat_id', filters.chatId)

    const loadItems = async () => {
      setLoading(true)
      try {
        const { data } = await api.get(`/marketplace?${params.toString()}`, {
          headers: authHeaders(token),
        })
        const list = Array.isArray(data.items) ? data.items : data
        setItems(list)
      } catch (error) {
        console.error('No se pudieron cargar los productos', error)
      } finally {
        setLoading(false)
      }
    }
    loadItems()
  }, [token, filters, refreshKey])

  const emptyState = !loading && items.length === 0

  const handleContact = async (item: MarketplaceItem) => {
    if (!token) return
    try {
      const { data } = await api.post(`/marketplace/items/${item.id}/contact`, null, {
        headers: authHeaders(token),
      })
      navigate(`/chats/${data.id}`)
    } catch (error) {
      console.error('No se pudo iniciar el chat con el vendedor', error)
    }
  }

  const categoryOptions = useMemo(() => {
    const map = new Map<number, Category>()
    categories.forEach((cat) => map.set(cat.id, cat))
    items.forEach((item) => {
      if (item.category_id && !map.has(item.category_id)) {
        map.set(item.category_id, { id: item.category_id, name: `Categoría #${item.category_id}` })
      }
    })
    return Array.from(map.values()).sort((a, b) => a.name.localeCompare(b.name))
  }, [categories, items])

  return (
    <div className="h-full flex flex-col gap-6 px-8 py-6 overflow-y-auto">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <MarketplaceFilters filters={filters} onChange={setFilters} categories={categoryOptions} chats={chats} />
        <div className="flex gap-3">
          <Link
            to="/marketplace/manage"
            className="glass-panel px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/20 text-sm font-semibold transition"
          >
            Mis productos
          </Link>
          <button
            onClick={() => setShowModal(true)}
            className="glass-panel px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/20 text-sm font-semibold transition"
          >
            Publicar producto
          </button>
        </div>
      </div>

      {loading && (
        <div className="text-center text-white/60 text-sm py-10">Cargando inventario…</div>
      )}

      {emptyState && (
        <div className="glass-panel rounded-3xl p-10 text-center space-y-4">
          <h3 className="text-2xl font-semibold">Nada por aquí todavía</h3>
          <p className="text-white/60">
            No hay productos que coincidan con tus filtros. Intenta ajustar los criterios o crea uno nuevo
            desde la conversación correspondiente.
          </p>
        </div>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-6">
        {items.map((item) => (
          <MarketplaceCard key={item.id} item={item} onContact={handleContact} />
        ))}
      </div>
      <SellItemModal
        open={showModal}
        onClose={() => setShowModal(false)}
        categories={categories}
        onCreated={() => {
          setShowModal(false)
          setRefreshKey((key) => key + 1)
        }}
      />
    </div>
  )
}


