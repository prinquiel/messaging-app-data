import { createContext, useContext, useEffect, useMemo, useState } from 'react'

type Locale = 'es' | 'en'

type LocaleContextValue = {
  locale: Locale
  toggleLocale: () => void
  setLocale: (locale: Locale) => void
}

const LocaleContext = createContext<LocaleContextValue | undefined>(undefined)

export function LocaleProvider({ children }: { children: React.ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>(() => {
    if (typeof window === 'undefined') return 'es'
    const stored = localStorage.getItem('locale') as Locale | null
    if (stored === 'es' || stored === 'en') return stored
    return 'es'
  })

  useEffect(() => {
    localStorage.setItem('locale', locale)
  }, [locale])

  const toggleLocale = () => setLocaleState((prev) => (prev === 'es' ? 'en' : 'es'))
  const setLocale = (next: Locale) => setLocaleState(next)

  const value = useMemo(
    () => ({
      locale,
      toggleLocale,
      setLocale,
    }),
    [locale],
  )

  return <LocaleContext.Provider value={value}>{children}</LocaleContext.Provider>
}

export function useLocale() {
  const ctx = useContext(LocaleContext)
  if (!ctx) {
    throw new Error('useLocale debe usarse dentro de LocaleProvider')
  }
  return ctx
}


