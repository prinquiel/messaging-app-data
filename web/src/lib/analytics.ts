import { Identify, identify, init, reset, setUserId, track as amplitudeTrack } from '@amplitude/analytics-browser'

type AnalyticsUser = {
  id: number
  email?: string
  username?: string
  fullName?: string
}

const API_KEY = import.meta.env.VITE_AMPLITUDE_API_KEY
const isBrowser = typeof window !== 'undefined'
const canUseAnalytics = Boolean(API_KEY && isBrowser)

let initPromise: Promise<void> | null = null

async function ensureAnalyticsClient() {
  if (!canUseAnalytics) return
  if (!initPromise) {
    initPromise = init(API_KEY!, undefined, {
      defaultTracking: {
        sessions: true,
        pageViews: false, // we handle page views manually to enrich the payload
      },
    })
      .then(() => undefined)
      .catch((error) => {
        console.error('No se pudo inicializar Amplitude', error)
        initPromise = null
        throw error
      })
  }
  return initPromise
}

export async function initAnalytics() {
  try {
    await ensureAnalyticsClient()
  } catch {
    // already logged in ensureAnalyticsClient
  }
}

export async function trackEvent(eventName: string, eventProperties?: Record<string, unknown>) {
  if (!canUseAnalytics) return
  try {
    await ensureAnalyticsClient()
    amplitudeTrack(eventName, eventProperties)
  } catch {
    // ignore, already logged
  }
}

export function trackPageView(path: string, properties?: Record<string, unknown>) {
  return trackEvent('page_view', {
    path,
    ...properties,
  })
}

export async function setAnalyticsUser(user: AnalyticsUser) {
  if (!canUseAnalytics) return
  try {
    await ensureAnalyticsClient()
    setUserId(String(user.id))
    const identifyPayload = new Identify()
    if (user.email) identifyPayload.set('email', user.email)
    if (user.username) identifyPayload.set('username', user.username)
    if (user.fullName) identifyPayload.set('full_name', user.fullName)
    identify(identifyPayload)
  } catch {
    // ignore, already logged
  }
}

export async function clearAnalyticsUser() {
  if (!canUseAnalytics) return
  try {
    await ensureAnalyticsClient()
    reset()
  } catch {
    // ignore, already logged
  }
}

