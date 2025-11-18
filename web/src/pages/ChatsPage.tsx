import { useState } from 'react'
import { motion } from 'framer-motion'
import EmptyState from '../components/EmptyState'
import { api, authHeaders } from '../lib/api'
import { useAuth } from '../auth/AuthContext'
import { useChatStore } from '../store/chatStore'

type UserSummary = {
  id: number
  username: string
  full_name: string
}

export default function ChatsPage() {
  const { token, userId } = useAuth()
  const { chats, setChats } = useChatStore()
  const [chatName, setChatName] = useState('')
  const [chatType, setChatType] = useState<'private' | 'group'>('private')
  const [creating, setCreating] = useState(false)
  const [error, setError] = useState('')
  const [userQuery, setUserQuery] = useState('')
  const [searching, setSearching] = useState(false)
  const [searchResults, setSearchResults] = useState<UserSummary[]>([])
  const [selectedUsers, setSelectedUsers] = useState<UserSummary[]>([])

  const handleSearch = async () => {
    if (!token || userQuery.trim().length < 2) return
    setSearching(true)
    try {
      const { data } = await api.get(`/users/search?query=${encodeURIComponent(userQuery)}`, {
        headers: authHeaders(token),
      })
      setSearchResults(data.filter((user: UserSummary) => user.id !== userId))
    } catch (err) {
      console.error(err)
    } finally {
      setSearching(false)
    }
  }

  const toggleUserSelection = (user: UserSummary) => {
    const exists = selectedUsers.some((u) => u.id === user.id)
    if (exists) {
      setSelectedUsers(selectedUsers.filter((u) => u.id !== user.id))
      return
    }
    if (chatType === 'private' && selectedUsers.length >= 1) return
    setSelectedUsers([...selectedUsers, user])
  }

  const handleCreateChat = async () => {
    if (!token) return
    if (selectedUsers.length === 0) {
      setError('Selecciona al menos un usuario')
      return
    }
    if (chatType === 'group' && selectedUsers.length < 2) {
      setError('Un grupo requiere al menos 2 usuarios además de ti')
      return
    }
    setCreating(true)
    setError('')
    try {
      const member_ids = selectedUsers.map((u) => u.id)
      const payload = {
        name: chatType === 'private' ? chatName || selectedUsers[0]?.full_name : chatName || 'Nuevo canal',
        chat_type: chatType,
        member_ids,
      }
      const { data } = await api.post('/me/chats', payload, { headers: authHeaders(token) })
      setChats([data, ...chats])
      setChatName('')
      setSelectedUsers([])
      setUserQuery('')
      setSearchResults([])
    } catch (err: any) {
      setError(err?.response?.data?.detail || 'No se pudo crear el chat')
    } finally {
      setCreating(false)
    }
  }

  return (
    <div className="h-full flex flex-col gap-6 px-10 py-8 overflow-y-auto">
      <div className="flex-1 flex flex-col items-center justify-center">
        <EmptyState />
      </div>
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        className="glass-panel rounded-3xl p-6 max-w-2xl mx-auto w-full space-y-4"
      >
        <div className="flex items-center justify-between flex-wrap gap-3">
          <div>
            <p className="text-xs uppercase tracking-[0.4em] text-white/60">Nuevo chat</p>
            <h3 className="text-xl font-semibold">Crea un canal</h3>
          </div>
          <div className="flex gap-2 bg-white/5 rounded-2xl p-1">
            {(['private', 'group'] as const).map((type) => (
              <button
                key={type}
                onClick={() => {
                  setChatType(type)
                  setSelectedUsers([])
                }}
                className={`px-3 py-1.5 rounded-2xl text-sm ${
                  chatType === type ? 'bg-white/20 font-semibold' : 'text-white/60'
                }`}
              >
                {type === 'private' ? 'Privado' : 'Grupo'}
              </button>
            ))}
          </div>
        </div>
        {error && <div className="text-sm text-rose-300">{error}</div>}
        <div className="space-y-2">
          <label className="text-xs uppercase tracking-[0.4em] text-white/50">Nombre</label>
          <input
            className="glass-input w-full p-3"
            placeholder={chatType === 'private' ? 'Chat con… (opcional)' : 'Equipo de producto'}
            value={chatName}
            onChange={(e) => setChatName(e.target.value)}
          />
        </div>
        <div className="space-y-2">
          <label className="text-xs uppercase tracking-[0.4em] text-white/50">Agregar usuarios</label>
          <div className="flex gap-2">
            <input
              className="glass-input flex-1 p-3"
              placeholder="Buscar por nombre o email"
              value={userQuery}
              onChange={(e) => setUserQuery(e.target.value)}
            />
            <button
              onClick={handleSearch}
              className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/20 transition text-sm"
              disabled={searching || userQuery.trim().length < 2}
            >
              {searching ? 'Buscando...' : 'Buscar'}
            </button>
          </div>
          {searchResults.length > 0 && (
            <div className="flex flex-wrap gap-2">
              {searchResults.map((user) => {
                const selected = selectedUsers.some((u) => u.id === user.id)
                return (
                  <button
                    key={user.id}
                    onClick={() => toggleUserSelection(user)}
                    className={`px-3 py-2 rounded-2xl text-sm border ${
                      selected ? 'bg-brand-500/30 border-brand-400' : 'bg-white/5 border-white/10'
                    }`}
                  >
                    {user.username} · {user.full_name}
                  </button>
                )
              })}
            </div>
          )}
        </div>
        {selectedUsers.length > 0 && (
          <div className="space-y-2">
            <p className="text-xs uppercase tracking-[0.4em] text-white/50">Participantes</p>
            <div className="flex flex-wrap gap-2">
              {selectedUsers.map((user) => (
                <span key={user.id} className="px-3 py-1.5 rounded-2xl text-sm bg-white/10">
                  {user.full_name} ({user.username})
                </span>
              ))}
            </div>
          </div>
        )}
        <button
          onClick={handleCreateChat}
          disabled={creating}
          className="w-full py-3 rounded-2xl bg-gradient-to-r from-brand-500 to-purple-500 font-semibold disabled:opacity-50"
        >
          {creating ? 'Creando...' : 'Crear chat'}
        </button>
      </motion.div>
    </div>
  )
}


