import { useLocale } from '../theme/LocaleProvider'

export default function LanguageToggle() {
  const { locale, toggleLocale } = useLocale()

  return (
    <button
      onClick={toggleLocale}
      className="glass-panel px-3 py-2 rounded-2xl text-xs font-semibold hover:bg-white/10 transition"
      aria-label="Cambiar idioma"
    >
      {locale === 'es' ? 'ES' : 'EN'}
    </button>
  )
}


