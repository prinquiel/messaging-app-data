import { fireEvent, render, screen } from '@testing-library/react'
import LanguageToggle from '../LanguageToggle'
import { LocaleProvider } from '../../theme/LocaleProvider'

describe('LanguageToggle', () => {
  it('cambia entre ES y EN', () => {
    render(
      <LocaleProvider>
        <LanguageToggle />
      </LocaleProvider>,
    )
    const button = screen.getByRole('button', { name: /cambiar idioma/i })
    expect(button).toHaveTextContent('ES')
    fireEvent.click(button)
    expect(button).toHaveTextContent('EN')
  })
})


