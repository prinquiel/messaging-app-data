import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import ListingModal from '../ListingModal'

const postMock = vi.fn()
const getMock = vi.fn().mockResolvedValue({ data: { items: [] } })

vi.mock('../../lib/api', () => ({
  api: {
    post: (...args: unknown[]) => postMock(...args),
    get: (...args: unknown[]) => getMock(...args),
  },
  authHeaders: () => ({}),
}))

vi.mock('../../auth/AuthContext', () => ({
  useAuth: () => ({
    token: 'test-token',
    userId: 1,
    login: vi.fn(),
    register: vi.fn(),
    logout: vi.fn(),
  }),
}))

describe('ListingModal', () => {
  beforeEach(() => {
    postMock.mockReset().mockResolvedValue({ data: {} })
  })

  it('convierte mensaje existente en producto', async () => {
    render(
      <ListingModal
        open
        onClose={() => undefined}
        message={{ id: 10, content: 'Hola', chatId: 4 }}
      />,
    )

    fireEvent.change(screen.getByPlaceholderText(/nombre del producto/i), {
      target: { value: 'Libro' },
    })
    fireEvent.change(screen.getByPlaceholderText(/describe tu producto/i), {
      target: { value: 'Muy bueno' },
    })
    fireEvent.change(screen.getByPlaceholderText('0.00'), {
      target: { value: '123' },
    })
    fireEvent.click(screen.getByRole('button', { name: /publicar/i }))

    await waitFor(() => {
      expect(postMock).toHaveBeenCalledWith(
        '/messages/10/sell',
        expect.objectContaining({
          title: 'Libro',
          chat_id: 4,
          price: 123,
          currency: 'CRC',
        }),
        expect.any(Object),
      )
    })
  })

  it('crea producto independiente desde el marketplace', async () => {
    render(<ListingModal open onClose={() => undefined} />)

    fireEvent.change(screen.getByPlaceholderText(/nombre del producto/i), {
      target: { value: 'Monitor' },
    })
    fireEvent.change(screen.getByPlaceholderText('Describe tu producto...'), {
      target: { value: '4k' },
    })
    fireEvent.change(screen.getByPlaceholderText('0.00'), {
      target: { value: '999' },
    })
    fireEvent.click(screen.getByRole('button', { name: /publicar/i }))

    await waitFor(() => {
      expect(postMock).toHaveBeenCalledWith(
        '/marketplace/items',
        expect.objectContaining({
          title: 'Monitor',
          price: 999,
          currency: 'CRC',
        }),
        expect.any(Object),
      )
    })
  })
})


