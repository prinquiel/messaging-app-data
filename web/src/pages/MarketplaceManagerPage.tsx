import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '../auth/AuthContext'
import { api, authHeaders } from '../lib/api'
import ListingModal from '../components/ListingModal'
import { MarketplaceItem } from '../components/MarketplaceCard'

type StatusFilter = 'all' | 'active' | 'sold' | 'pending' | 'cancelled'

const STATUS_OPTIONS: { key: StatusFilter; label: string }[] = [
  { key: 'all', label: 'Todos' },
  { key: 'active', label: 'Activos' },
  { key: 'sold', label: 'Vendidos' },
  { key: 'pending', label: 'Pendientes' },
  { key: 'cancelled', label: 'Cancelados' },
]

export default function MarketplaceManagerPage() {
  const { token, userId } = useAuth()
  const [items, setItems] = useState<MarketplaceItem[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all')
  const [busyItemId, setBusyItemId] = useState<number | null>(null)
  const [showModal, setShowModal] = useState(false)
  const [refreshKey, setRefreshKey] = useState(0)

  useEffect(() => {
    if (!token || !userId) return
    const loadItems = async () => {
      setLoading(true)
      setError('')
      try {
        const params = new URLSearchParams({
          page: '1',
          page_size: '100',
          seller_id: String(userId),
          include_total: 'true',
        })
        const { data } = await api.get(`/marketplace?${params.toString()}`, {
          headers: authHeaders(token),
        })
        const list = Array.isArray(data.items) ? data.items : data
        setItems(list)
      } catch (err) {
        console.error('No se pudo cargar el inventario', err)
        setError('No se pudo cargar tus publicaciones.')
      } finally {
        setLoading(false)
      }
    }
    loadItems()
  }, [token, userId, refreshKey])

  const filteredItems = useMemo(() => {
    if (statusFilter === 'all') return items
    return items.filter((item) => item.status === statusFilter)
  }, [items, statusFilter])

  const summary = useMemo(() => {
    return items.reduce(
      (acc, item) => {
        acc.total += 1
        acc[item.status as keyof typeof acc] = (acc[item.status as keyof typeof acc] || 0) + 1
        return acc
      },
      { total: 0, active: 0, sold: 0, pending: 0, cancelled: 0 } as Record<string, number>,
    )
  }, [items])

  const handleStatusChange = async (itemId: number, status: 'active' | 'sold' | 'pending' | 'cancelled') => {
    if (!token) return
    setBusyItemId(itemId)
    try {
      const { data } = await api.put(
        `/marketplace/${itemId}`,
        { status },
        {
          headers: authHeaders(token),
        },
      )
      setItems((prev) => prev.map((item) => (item.id === itemId ? { ...item, status: data.status } : item)))
    } catch (err) {
      console.error('No se pudo actualizar el estado', err)
      setError('No se pudo actualizar el estado del producto.')
    } finally {
      setBusyItemId(null)
    }
  }

  return (
    <div className="h-full flex flex-col gap-6 px-8 py-6 overflow-y-auto">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <p className="text-xs uppercase tracking-[0.4em] text-white/60">Marketplace</p>
          <h2 className="text-2xl font-semibold">Mis publicaciones</h2>
        </div>
        <div className="flex gap-3 flex-wrap">
          <Link
            to="/marketplace"
            className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/20 transition text-sm font-semibold"
          >
            Ver catálogo
          </Link>
          <button
            onClick={() => setShowModal(true)}
            className="px-4 py-2 rounded-2xl bg-gradient-to-r from-brand-500 to-purple-500 font-semibold"
          >
            Nuevo producto
          </button>
        </div>
      </div>

      <div className="glass-panel rounded-3xl p-5 grid grid-cols-2 md:grid-cols-4 gap-4">
        <SummaryCard label="Publicados" value={summary.total} />
        <SummaryCard label="Activos" value={summary.active} tone="emerald" />
        <SummaryCard label="Vendidos" value={summary.sold} tone="violet" />
        <SummaryCard label="Pendientes" value={summary.pending} tone="amber" />
      </div>

      <div className="glass-panel rounded-3xl p-4 flex flex-wrap gap-2">
        {STATUS_OPTIONS.map((option) => (
          <button
            key={option.key}
            onClick={() => setStatusFilter(option.key)}
            className={`px-4 py-2 rounded-2xl text-sm ${
              statusFilter === option.key ? 'bg-white/20 font-semibold' : 'bg-white/5 text-white/70'
            }`}
          >
            {option.label}
          </button>
        ))}
      </div>

      {error && <div className="text-sm text-rose-300">{error}</div>}

      {loading ? (
        <div className="flex-1 flex items-center justify-center text-white/70 text-sm">Cargando inventario…</div>
      ) : filteredItems.length === 0 ? (
        <div className="glass-panel rounded-3xl p-10 text-center">
          <h3 className="text-xl font-semibold mb-2">Sin productos aquí</h3>
          <p className="text-white/60">Publica un artículo o cambia el filtro para ver más resultados.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {filteredItems.map((item) => (
            <div key={item.id} className="glass-panel rounded-3xl p-5 flex flex-col gap-4">
              <div>
                <p className="text-xs uppercase tracking-[0.4em] text-white/50 mb-1">#{item.id}</p>
                <h3 className="text-xl font-semibold">{item.title}</h3>
                <p className="text-sm text-white/60 line-clamp-2">{item.description}</p>
              </div>
              <div className="flex items-baseline gap-2">
                <p className="text-2xl font-bold">
                  {new Intl.NumberFormat('es-CR', { style: 'currency', currency: item.currency || 'CRC' }).format(
                    item.price,
                  )}
                </p>
                <span className="text-xs text-white/60">{item.is_negotiable ? 'Negociable' : 'Fijo'}</span>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span
                  className={`px-3 py-1 rounded-full text-xs uppercase tracking-[0.3em] ${
                    item.status === 'sold'
                      ? 'bg-rose-500/20 text-rose-200'
                      : item.status === 'active'
                        ? 'bg-emerald-500/20 text-emerald-200'
                        : 'bg-white/10 text-white/70'
                  }`}
                >
                  {item.status}
                </span>
                <p className="text-white/60">
                  Publicado el {new Date(item.created_at ?? '').toLocaleDateString('es-CR')}
                </p>
              </div>
              <div className="flex gap-2 flex-wrap">
                <button
                  disabled={busyItemId === item.id || item.status === 'sold'}
                  onClick={() => handleStatusChange(item.id, 'sold')}
                  className="flex-1 px-3 py-2 rounded-2xl bg-white/10 hover:bg-white/20 text-sm font-semibold disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {busyItemId === item.id ? 'Actualizando…' : 'Marcar como vendido'}
                </button>
                <button
                  disabled={busyItemId === item.id || item.status === 'active'}
                  onClick={() => handleStatusChange(item.id, 'active')}
                  className="px-3 py-2 rounded-2xl border border-white/20 text-sm font-semibold disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  Re-activar
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      <ListingModal
        open={showModal}
        onClose={() => setShowModal(false)}
        onCreated={() => {
          setShowModal(false)
          setRefreshKey((key) => key + 1)
        }}
      />
    </div>
  )
}

type SummaryCardProps = {
  label: string
  value: number
  tone?: 'emerald' | 'violet' | 'amber'
}

function SummaryCard({ label, value, tone }: SummaryCardProps) {
  const toneClass =
    tone === 'emerald'
      ? 'text-emerald-200 bg-emerald-500/10 border-emerald-400/40'
      : tone === 'violet'
        ? 'text-violet-200 bg-violet-500/10 border-violet-400/40'
        : tone === 'amber'
          ? 'text-amber-200 bg-amber-500/10 border-amber-400/40'
          : 'text-white border-white/10'
  return (
    <div className={`rounded-2xl border px-4 py-3 flex flex-col gap-1 ${toneClass}`}>
      <p className="text-xs uppercase tracking-[0.4em] text-white/60">{label}</p>
      <p className="text-2xl font-semibold">{value}</p>
    </div>
  )
}

