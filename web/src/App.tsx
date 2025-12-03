import { useEffect } from 'react'
import { Navigate, Route, Routes, useLocation } from 'react-router-dom'
import { useAuth } from './auth/AuthContext'
import LoginPage from './pages/LoginPage'
import RegisterPage from './pages/RegisterPage'
import ChatsPage from './pages/ChatsPage'
import ChatView from './pages/ChatView'
import MarketplacePage from './pages/MarketplacePage'
import MarketplaceManagerPage from './pages/MarketplaceManagerPage'
import AppShell from './components/AppShell'
import { trackPageView } from './lib/analytics'

function PrivateLayout() {
  const { token } = useAuth()
  if (!token) return <Navigate to="/login" replace />
  return <AppShell />
}

export default function App() {
  const location = useLocation()

  useEffect(() => {
    void trackPageView(location.pathname, {
      search: location.search || undefined,
      title: document.title,
    })
  }, [location.pathname, location.search])

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/register" element={<RegisterPage />} />
      <Route element={<PrivateLayout />}>
        <Route index element={<ChatsPage />} />
        <Route path="/chats/:id" element={<ChatView />} />
        <Route path="/marketplace" element={<MarketplacePage />} />
        <Route path="/marketplace/manage" element={<MarketplaceManagerPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/" />} />
    </Routes>
  )
}

