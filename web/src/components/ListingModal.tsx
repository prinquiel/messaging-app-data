import { useEffect, useState } from 'react'
import { useAuth } from '../auth/AuthContext'
import { api, authHeaders } from '../lib/api'

type Category = {
  id: number
  name: string
}

type Props = {
  open: boolean
  onClose: () => void
  categories?: Category[]
  message?: { id: number; content: string; chatId: number }
  onCreated?: () => void
}

const USD_TO_CRC = 540

export default function SellItemModal({ open, onClose, categories: initialCategories = [], message, onCreated }: Props) {
  const { token, userId } = useAuth()
  const [categories, setCategories] = useState<Category[]>(initialCategories)
  const [title, setTitle] = useState('')
  const [description, setDescription] = useState('')
  const [price, setPrice] = useState('')
  const [categoryId, setCategoryId] = useState('')
  const [messageBody, setMessageBody] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    if (initialCategories.length) {
      setCategories(initialCategories)
    }
  }, [initialCategories])

  useEffect(() => {
    if (!open) return
    setTitle('')
    setDescription(message?.content ?? '')
    setPrice('')
    setCategoryId('')
    setMessageBody('')
    setError('')
  }, [open, message])

  useEffect(() => {
    if (!open) return
    if (initialCategories.length) return
    const loadCategories = async () => {
      if (!token) return
      try {
        const { data } = await api.get('/categories?page=1&page_size=100', {
          headers: authHeaders(token),
        })
        const list = Array.isArray(data.items) ? data.items : data
        setCategories(list)
      } catch (err) {
        console.error('No se pudieron cargar las categorías', err)
      }
    }
    loadCategories()
  }, [open, token, initialCategories.length])

  if (!open) return null

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!token || submitting) return
    if (!title.trim()) {
      setError('Agrega un título')
      return
    }
    const numericPrice = Number(price)
    if (!numericPrice || numericPrice <= 0) {
      setError('Precio inválido')
      return
    }
    setSubmitting(true)
    setError('')
    try {
      if (message) {
        const payload = {
          message_id: message.id,
          chat_id: message.chatId,
          title: title.trim(),
          description: description || null,
          price: numericPrice,
          currency: 'CRC',
          category_id: categoryId ? Number(categoryId) : null,
        }
        await api.post(`/messages/${message.id}/sell`, payload, {
          headers: authHeaders(token),
        })
        onCreated?.()
        onClose()
        return
      }

      const standalonePayload = {
        title: title.trim(),
        description: description || null,
        price: numericPrice,
        currency: 'CRC',
        category_id: categoryId ? Number(categoryId) : null,
        message_content: messageBody.trim() || undefined,
      }
      await api.post('/marketplace/items', standalonePayload, {
        headers: authHeaders(token),
      })
      onCreated?.()
      onClose()
    } catch (err: any) {
      console.error('Error al publicar el producto', err)
      setError(err?.response?.data?.detail || 'Error al publicar el producto')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur">
      <div className="bg-slate-900 text-white rounded-3xl w-full max-w-2xl p-6 space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs uppercase tracking-[0.4em] text-white/60">Marketplace</p>
            <h3 className="text-2xl font-semibold">
              {message ? 'Publicar desde el chat' : 'Nuevo producto'}
            </h3>
          </div>
          <button onClick={onClose} className="text-white/60 hover:text-white text-sm">
            Cerrar
          </button>
        </div>
        <form className="space-y-4" onSubmit={handleSubmit}>
          {message && (
            <div className="glass-panel rounded-2xl p-4 text-sm text-white/70">
              <p className="uppercase text-[10px] tracking-[0.4em] text-white/50 mb-2">Mensaje</p>
              <p>{message.content}</p>
            </div>
          )}
          {!message && (
            <div className="glass-panel rounded-2xl p-4 text-sm text-white/70">
              <p className="uppercase text-[10px] tracking-[0.4em] text-white/50 mb-2">¿Cómo funciona?</p>
              <p>
                El producto se publicará en tu catálogo y quedará registrado en un chat privado invisible para los
                demás. Cuando alguien haga clic en “Coordinar por chat”, se abrirá un DM directo contigo.
              </p>
            </div>
          )}
          <div className="space-y-2">
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Título</label>
            <input
              className="glass-input w-full p-3"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              placeholder="Nombre del producto"
            />
          </div>
          <div className="space-y-2">
            <label className="text-xs uppercase tracking-[0.4em] text-white/50">Descripción</label>
            <textarea
              className="glass-input w-full p-3 min-h-[120px]"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Describe tu producto..."
            />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="space-y-2">
              <label className="text-xs uppercase tracking-[0.4em] text-white/50">Precio (₡)</label>
              <input
                type="number"
                min="0"
                step="0.01"
                className="glass-input w-full p-3"
                value={price}
                onChange={(e) => setPrice(e.target.value)}
                placeholder="0.00"
              />
            </div>
            <div className="space-y-2">
              <label className="text-xs uppercase tracking-[0.4em] text-white/50">Categoría</label>
              <select
                className="glass-input w-full p-3 bg-transparent"
                value={categoryId}
                onChange={(e) => setCategoryId(e.target.value)}
              >
                <option value="">Sin categoría</option>
                {categories.map((category) => (
                  <option key={category.id} value={String(category.id)}>
                    {category.name}
                  </option>
                ))}
              </select>
            </div>
          </div>
          {!message && (
            <div className="space-y-2">
              <label className="text-xs uppercase tracking-[0.4em] text-white/50">
                Mensaje interno (opcional)
              </label>
              <textarea
                className="glass-input w-full p-3 min-h-[80px]"
                value={messageBody}
                onChange={(e) => setMessageBody(e.target.value)}
                placeholder="Texto que quedará en tu chat interno"
              />
            </div>
          )}
          {error && <div className="text-sm text-rose-300">{error}</div>}
          <div className="flex justify-end gap-3">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/20 transition"
            >
              Cancelar
            </button>
            <button
              type="submit"
              disabled={submitting}
              className="px-4 py-2 rounded-2xl bg-gradient-to-r from-brand-500 to-purple-500 font-semibold disabled:opacity-50"
            >
              {submitting ? 'Publicando…' : 'Publicar'}
            </button>
          </div>
          {message && (
            <p className="text-xs text-white/50">
              El precio se guarda en colones. Si cambias a inglés, se mostrará en dólares (~₡{USD_TO_CRC} = $1).
            </p>
          )}
        </form>
      </div>
    </div>
  )
}

