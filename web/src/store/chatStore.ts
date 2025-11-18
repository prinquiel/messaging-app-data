import { create } from 'zustand'

export type ChatSummary = {
  id: number
  name?: string | null
  chat_type: string
  created_at: string
  last_message?: string
  unread?: number
}

type ChatStore = {
  chats: ChatSummary[]
  activeChatId?: number
  setChats: (chats: ChatSummary[]) => void
  setActiveChat: (id?: number) => void
  markAsRead: (id: number) => void
}

export const useChatStore = create<ChatStore>((set) => ({
  chats: [],
  activeChatId: undefined,
  setChats: (chats) => set({ chats }),
  setActiveChat: (id) => set({ activeChatId: id }),
  markAsRead: (id) =>
    set((state) => ({
      chats: state.chats.map((chat) =>
        chat.id === id ? { ...chat, unread: 0 } : chat
      ),
    })),
}))


