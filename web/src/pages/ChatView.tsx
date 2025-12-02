import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useParams } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { useAuth } from '../auth/AuthContext'
import { useWebSocket } from '../hooks/useWebSocket'
import { api, authHeaders, API_URL } from '../lib/api'
import ChatHeader from '../components/ChatHeader'
import MessageBubble from '../components/MessageBubble'
import ChatComposer from '../components/ChatComposer'
import TypingIndicator from '../components/TypingIndicator'
import UnreadSeparator from '../components/UnreadSeparator'
import { useChatStore } from '../store/chatStore'
import SellItemModal from '../components/ListingModal'

type ChatMessage = {
  id: number
  content: string
  sender_id: number
  chat_id: number
  sent_at: string
  isNew?: boolean
}

type ChatMember = {
  id: number
  username?: string
  full_name?: string
}

export default function ChatView() {
  const { id } = useParams()
  const chatId = Number(id)
  const { token, userId, user } = useAuth()
  const { chats, setActiveChat, markAsRead } = useChatStore()
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [chatMeta, setChatMeta] = useState<{ name?: string; chat_type?: string; members?: ChatMember[] }>({})
  const [input, setInput] = useState('')
  const [showTyping, setShowTyping] = useState(false)
  const scrollerRef = useRef<HTMLDivElement | null>(null)
  const unreadInjectedRef = useRef(false)
  const typingIndicatorEnabled = true
  const [onlineUserIds, setOnlineUserIds] = useState<number[]>([])
  const typingTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const typingSignalRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const [sellMessage, setSellMessage] = useState<ChatMessage | null>(null)

  useEffect(() => {
    return () => {
      if (typingTimeoutRef.current) clearTimeout(typingTimeoutRef.current)
      if (typingSignalRef.current) clearTimeout(typingSignalRef.current)
    }
  }, [])

  const currentChat = chats.find((c) => c.id === chatId)

  useEffect(() => {
    setChatMeta({})
  }, [chatId])

  useEffect(() => {
    if (!currentChat) return
    setChatMeta((prev) => ({
      ...prev,
      name: currentChat.name ?? prev.name,
      chat_type: currentChat.chat_type ?? prev.chat_type,
    }))
  }, [currentChat])

  useEffect(() => {
    if (!chatId || !token) return
    setActiveChat(chatId)
    markAsRead(chatId)
    const loadHistory = async () => {
      try {
        const { data } = await api.get(`/chats/${chatId}/messages?page=1&page_size=200`, {
          headers: authHeaders(token),
        })
        const items = Array.isArray(data.items) ? data.items : data
        const sorted = [...items].reverse()
        setMessages(sorted)
        unreadInjectedRef.current = false
      } catch (error) {
        console.error('No se pudieron cargar los mensajes', error)
      }
    }
    loadHistory()
  }, [chatId, token, setActiveChat, markAsRead])

  useEffect(() => {
    if (!token || !chatId) return
    let cancelled = false
    const loadChat = async () => {
      try {
        const { data } = await api.get(`/chats/${chatId}`, { headers: authHeaders(token) })
        if (cancelled) return
        setChatMeta((prev) => ({
          ...prev,
          name: data.name ?? prev.name,
          chat_type: data.chat_type ?? prev.chat_type,
          members: data.members,
        }))
      } catch (error) {
        console.error('No se pudo obtener el chat', error)
      }
    }
    loadChat()
    return () => {
      cancelled = true
    }
  }, [chatId, token])

  const wsUrl = useMemo(() => {
    if (!chatId || !token) return null
    const url = new URL(`${API_URL.replace("http", "ws")}/ws/chats/${chatId}`)
    url.searchParams.set('token', token)
    return url.toString()
  }, [chatId, token])

  const handleIncoming = useCallback(
    (data: any) => {
      if (data?.type === 'typing') {
        if (data.sender_id !== userId) {
          if (typingTimeoutRef.current) clearTimeout(typingTimeoutRef.current)
          setShowTyping(Boolean(data.is_typing))
          if (data.is_typing) {
            typingTimeoutRef.current = setTimeout(() => setShowTyping(false), 3500)
          }
        }
        return
      }
      if (data?.type === 'presence') {
        setOnlineUserIds(data.user_ids || [])
        return
      }
      setMessages((prev) => [...prev, { ...data, isNew: true }])
      setShowTyping(false)
    },
    [userId],
  )

  const wsOptions = useMemo(() => ({ onMessage: handleIncoming }), [handleIncoming])
  const { connected, send } = useWebSocket(wsUrl, wsOptions)

  const handleSend = () => {
    if (!input.trim()) return
    send({ content: input.trim() })
    setInput('')
    if (typingIndicatorEnabled) {
      send({ type: 'typing', is_typing: false })
      setShowTyping(false)
    }
  }

  const handleTypingChange = (typing: boolean) => {
    if (!typingIndicatorEnabled) return
    send({ type: 'typing', is_typing: typing })
    if (typingSignalRef.current) clearTimeout(typingSignalRef.current)
    if (typing) {
      typingSignalRef.current = setTimeout(() => {
        send({ type: 'typing', is_typing: false })
      }, 2000)
    }
  }

  useEffect(() => {
    if (!scrollerRef.current) return
    scrollerRef.current.scrollTo({ top: scrollerRef.current.scrollHeight, behavior: 'smooth' })
  }, [messages])

  const title = currentChat?.name || chatMeta.name || `Chat #${chatId}`
  const subtitle = currentChat?.chat_type || chatMeta.chat_type
  const members = chatMeta.members || []
  const peers = members.filter((member) => member.id !== userId)
  const activePeers = peers.filter((member) => onlineUserIds.includes(member.id))
  const remoteIndicatorOnline = activePeers.length > 0

  let unreadInserted = false

  const memberLookup = useMemo(() => {
    const map = new Map<number, string>()
    members.forEach((member) => {
      map.set(member.id, member.full_name || member.username || `Usuario ${member.id}`)
    })
    return map
  }, [members])

  const resolveSenderName = useCallback(
    (senderId: number) => {
      if (senderId === userId) {
        return user?.full_name || user?.username || 'Tú'
      }
      return memberLookup.get(senderId) || `Usuario ${senderId}`
    },
    [memberLookup, userId, user],
  )

  return (
    <div className="h-full flex flex-col">
      <ChatHeader chatId={chatId} title={title} subtitle={subtitle} />

      <div ref={scrollerRef} className="flex-1 overflow-y-auto scroll-area px-8 py-6 space-y-4">
        <AnimatePresence initial={false}>
          {messages.map((msg) => {
            let showDivider = false
            if (msg.isNew && !unreadInjectedRef.current) {
              showDivider = true
              unreadInjectedRef.current = true
            }
            return (
              <div key={msg.id}>
                {showDivider && <UnreadSeparator />}
                <MessageBubble
                  content={msg.content}
                  senderName={resolveSenderName(msg.sender_id)}
                  timestamp={msg.sent_at}
                  isOwn={msg.sender_id === userId}
                  onSell={msg.sender_id === userId ? () => setSellMessage(msg) : undefined}
                />
              </div>
            )
          })}
        </AnimatePresence>
        {typingIndicatorEnabled && showTyping && <TypingIndicator />}
      </div>

      <div className="px-8 pb-6 space-y-3">
        <div className="flex items-center gap-3 text-xs text-white/60">
          <span className={`h-2 w-2 rounded-full ${remoteIndicatorOnline ? 'bg-emerald-400' : 'bg-white/30'}`} />
          <span className="flex items-center gap-1">
            <span className={`h-2 w-2 rounded-full ${connected ? 'bg-sky-400' : 'bg-rose-400'}`} />
            {connected ? 'WS activo' : 'Reconectando WS'}
          </span>
        </div>
        <ChatComposer
          value={input}
          onChange={setInput}
          onSend={handleSend}
          onTypingChange={typingIndicatorEnabled ? handleTypingChange : undefined}
        />
      </div>
      <SellItemModal
        open={!!sellMessage}
        onClose={() => setSellMessage(null)}
        message={
          sellMessage
            ? { id: sellMessage.id, content: sellMessage.content, chatId: sellMessage.chat_id }
            : undefined
        }
        onCreated={() => setSellMessage(null)}
      />
    </div>
  )
}


