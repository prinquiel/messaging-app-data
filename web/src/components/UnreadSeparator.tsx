export default function UnreadSeparator() {
  return (
    <div className="flex items-center gap-3 my-6">
      <div className="flex-1 h-px bg-white/10" />
      <span className="text-xs uppercase tracking-[0.4em] text-rose-300">Nuevos</span>
      <div className="flex-1 h-px bg-white/10" />
    </div>
  )
}


