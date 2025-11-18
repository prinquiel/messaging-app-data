import { useEffect, useRef, useState } from 'react'

type Options = {
  onMessage?: (payload: any) => void
}

export function useWebSocket(url: string | null, options?: Options) {
  const wsRef = useRef<WebSocket | null>(null)
  const [connected, setConnected] = useState(false)
  const [messages, setMessages] = useState<any[]>([])

  useEffect(() => {
    if (!url) return
    const ws = new WebSocket(url)
    wsRef.current = ws
    ws.onopen = () => setConnected(true)
    ws.onclose = () => setConnected(false)
    ws.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data)
        setMessages((prev) => [...prev, data])
        options?.onMessage?.(data)
      } catch {
        setMessages((prev) => [...prev, { type: 'text', content: ev.data }])
      }
    }
    return () => {
      ws.close()
    }
  }, [url, options])

  const send = (data: any) => {
    wsRef.current?.send(typeof data === 'string' ? data : JSON.stringify(data))
  }

  return { connected, messages, send }
}


