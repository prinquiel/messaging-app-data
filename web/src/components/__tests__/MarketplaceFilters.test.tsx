import { fireEvent, render, screen } from '@testing-library/react'
import MarketplaceFilters, { MarketplaceFilters as FilterState } from '../MarketplaceFilters'

const baseFilters: FilterState = {
  search: '',
  categoryId: '',
  chatId: '',
}

const categories = [
  { id: 1, name: 'Tecnología' },
  { id: 2, name: 'Hogar' },
]

const chats = [
  { id: 10, name: 'Equipo' },
  { id: 11, name: 'Ventas' },
]

describe('MarketplaceFilters', () => {
  it('actualiza búsqueda y categoría', () => {
    const handleChange = vi.fn()
    render(
      <MarketplaceFilters
        filters={baseFilters}
        onChange={handleChange}
        categories={categories}
        chats={chats}
      />,
    )

    fireEvent.change(screen.getByPlaceholderText(/título, descripción/i), {
      target: { value: 'Laptop' },
    })
    expect(handleChange).toHaveBeenCalledWith({ ...baseFilters, search: 'Laptop' })

    fireEvent.change(screen.getByLabelText(/categoría/i), { target: { value: '2' } })
    expect(handleChange).toHaveBeenCalledWith({ ...baseFilters, categoryId: '2' })
  })
})


