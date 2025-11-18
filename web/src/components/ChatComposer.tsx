import { useRef } from 'react'
import { Paperclip, SendHorizonal, SmilePlus } from 'lucide-react'

type Props = {
  value: string
  onChange: (value: string) => void
  onSend: () => void
  disabled?: boolean
  onTypingChange?: (isTyping: boolean) => void
  hint?: string
}

export default function ChatComposer({ value, onChange, onSend, disabled, onTypingChange, hint }: Props) {
  const textareaRef = useRef<HTMLTextAreaElement | null>(null)

  const autoResize = () => {
    const textarea = textareaRef.current
    if (!textarea) return
    textarea.style.height = 'auto'
    textarea.style.height = `${Math.min(textarea.scrollHeight, 180)}px`
  }

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    onChange(e.target.value)
    autoResize()
    onTypingChange?.(!!e.target.value)
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      onSend()
    }
  }

  return (
    <div className="glass-panel rounded-3xl p-4 flex flex-col gap-3">
      <div className="flex items-center gap-2 text-white/60 text-sm">
        <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
        {hint ?? 'Cifrado activo · Presiona Enter para enviar'}
      </div>
      <div className="flex items-end gap-3">
        <button className="glass-panel p-3 rounded-2xl hover:bg-white/10 transition">
          <Paperclip className="w-5 h-5" />
        </button>
        <textarea
          ref={textareaRef}
          value={value}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          rows={1}
          className="flex-1 glass-input text-sm text-white resize-none p-3 min-h-[48px] max-h-[180px]"
          placeholder="Escribe un mensaje..."
          disabled={disabled}
        />
        <button className="glass-panel p-3 rounded-2xl hover:bg-white/10 transition">
          <SmilePlus className="w-5 h-5" />
        </button>
        <button
          onClick={onSend}
          disabled={!value.trim() || disabled}
          className="p-3 rounded-2xl bg-gradient-to-br from-brand-500 to-purple-600 disabled:opacity-40"
        >
          <SendHorizonal className="w-5 h-5" />
        </button>
      </div>
    </div>
  )
}


