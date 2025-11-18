type Props = {
  id: number
  name?: string | null
  active?: boolean
}

const palette = [
  'from-indigo-500 to-indigo-700',
  'from-violet-500 to-purple-700',
  'from-sky-500 to-blue-700',
  'from-teal-500 to-emerald-700',
]

export default function ChatAvatar({ id, name, active }: Props) {
  const gradient = palette[id % palette.length]
  const initial = name?.charAt(0)?.toUpperCase() ?? '#'

  return (
    <div
      className={`h-12 w-12 rounded-2xl flex items-center justify-center text-lg font-semibold bg-gradient-to-br ${gradient} ${
        active ? 'shadow-lg shadow-purple-500/40' : ''
      }`}
    >
      {initial}
    </div>
  )
}


