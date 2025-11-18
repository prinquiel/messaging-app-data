import { Navigate, Route, Routes } from 'react-router-dom'
import { useAuth } from './auth/AuthContext'
import LoginPage from './pages/LoginPage'
import RegisterPage from './pages/RegisterPage'
import ChatsPage from './pages/ChatsPage'
import ChatView from './pages/ChatView'
import MarketplacePage from './pages/MarketplacePage'
import AppShell from './components/AppShell'

function PrivateLayout() {
  const { token } = useAuth()
  if (!token) return <Navigate to="/login" replace />
  return <AppShell />
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/register" element={<RegisterPage />} />
      <Route element={<PrivateLayout />}>
        <Route index element={<ChatsPage />} />
        <Route path="/chats/:id" element={<ChatView />} />
        <Route path="/marketplace" element={<MarketplacePage />} />
      </Route>
      <Route path="*" element={<Navigate to="/" />} />
    </Routes>
  )
}

