import { render, screen } from '@testing-library/react'
import MarketplaceCard, { MarketplaceItem } from '../MarketplaceCard'

const mockLocale = { locale: 'es' as 'es' | 'en' }
vi.mock('../../theme/LocaleProvider', () => ({
  useLocale: () => mockLocale,
}))

const baseItem: MarketplaceItem = {
  id: 1,
  title: 'Guitarra',
  price: 100,
  currency: 'USD',
  status: 'active',
  chat_id: 1,
  seller_id: 1,
}

describe('MarketplaceCard', () => {
  it('muestra precios en colones cuando el idioma es español', () => {
    mockLocale.locale = 'es'
    render(<MarketplaceCard item={baseItem} />)
    expect(screen.getByText(/guitarra/i)).toBeInTheDocument()
    const price = screen.getByText((content) => content.includes('₡'))
    expect(price).toHaveTextContent('₡54')
  })

  it('muestra precios en dólares cuando el idioma es inglés', () => {
    mockLocale.locale = 'en'
    render(<MarketplaceCard item={baseItem} />)
    const price = screen.getByText((content) => content.includes('$'))
    expect(price).toHaveTextContent('$100')
  })
})


