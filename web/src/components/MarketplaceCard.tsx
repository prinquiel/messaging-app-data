import { motion } from 'framer-motion'
import { useLocale } from '../theme/LocaleProvider'

export type MarketplaceItem = {
  id: number
  title: string
  description?: string | null
  price: number
  currency: string
  status: string
  chat_id: number
  seller_id: number
  category_id?: number | null
  image_urls?: string[] | null
  created_at?: string
}

type Props = {
  item: MarketplaceItem
  onContact?: (item: MarketplaceItem) => void
}

const statusStyles: Record<string, string> = {
  active: 'bg-emerald-500/15 text-emerald-300 border-emerald-400/30',
  sold: 'bg-rose-500/15 text-rose-200 border-rose-400/30',
  cancelled: 'bg-slate-500/15 text-slate-300 border-slate-400/30',
  pending: 'bg-amber-500/15 text-amber-200 border-amber-400/30',
}

const USD_TO_CRC = 540

export default function MarketplaceCard({ item, onContact }: Props) {
  const { locale } = useLocale()
  const image =
    Array.isArray(item.image_urls) && item.image_urls.length
      ? item.image_urls[0]
      : `https://picsum.photos/seed/market-${item.id}/640/360`

  const statusClass = statusStyles[item.status] || 'bg-white/10 text-white border-white/10'
  const basePriceUSD =
    item.currency && item.currency.toUpperCase() === 'CRC'
      ? item.price / USD_TO_CRC
      : item.price
  const displayValue =
    locale === 'es' ? basePriceUSD * USD_TO_CRC : basePriceUSD
  const currencyCode = locale === 'es' ? 'CRC' : 'USD'
  const localeCode = locale === 'es' ? 'es-CR' : 'en-US'
  const formattedPrice = new Intl.NumberFormat(localeCode, {
    style: 'currency',
    currency: currencyCode,
    maximumFractionDigits: locale === 'es' ? 0 : 2,
  }).format(displayValue)

  return (
    <motion.article
      layout
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      className="glass-panel rounded-3xl overflow-hidden flex flex-col"
    >
      <div className="relative h-52 overflow-hidden">
        <img src={image} alt={item.title} className="w-full h-full object-cover" />
        <span className={`absolute top-4 right-4 px-3 py-1 rounded-full text-xs font-semibold border ${statusClass}`}>
          {item.status}
        </span>
      </div>
      <div className="p-5 flex flex-col gap-4 flex-1">
        <div>
          <h3 className="text-xl font-semibold">{item.title}</h3>
          <p className="text-sm text-white/60 line-clamp-3">{item.description}</p>
        </div>
        <div className="flex items-center justify-between">
          <div>
            <p className="text-2xl font-bold">{formattedPrice}</p>
            <p className="text-xs uppercase tracking-[0.4em] text-white/50">ID #{item.id}</p>
          </div>
          <div className="text-right text-xs text-white/60 space-y-1">
            <p>Vendedor · #{item.seller_id}</p>
            <p>Chat · #{item.chat_id}</p>
          </div>
        </div>
        {onContact && (
          <button
            onClick={() => onContact(item)}
            className="w-full inline-flex justify-center px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/20 text-sm font-semibold transition"
          >
            Coordinar por chat
          </button>
        )}
      </div>
    </motion.article>
  )
}


