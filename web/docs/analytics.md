# Analitica con Amplitude

Este frontend usa el SDK oficial `@amplitude/analytics-browser` para capturar eventos sin bloquear la UI. Toda la configuracion vive en `src/lib/analytics.ts`.

## Configuracion rapida

1. Instala dependencias (ya se agrego el SDK al `package.json`): `npm install`.
2. Crea un archivo `web/.env.local` (no se versiona) con tu clave de proyecto:
   ```
   VITE_API_URL=http://localhost:8000
   VITE_AMPLITUDE_API_KEY=tu_clave
   ```
3. Arranca el frontend con `npm run dev`. Si la variable no existe, el SDK queda silenciado automaticamente.

## Eventos incluidos

- `page_view`: se dispara en cada cambio de ruta desde `App.tsx` e incluye `path`, `search` y `title`.
- `login_success` y `register_success`: se envian desde `AuthContext` despues de un login/registro exitoso (propiedad `method` = `credentials`).
- `logout`: se emite antes de cerrar sesion.

El `AuthContext` tambien sincroniza la identidad del usuario (id, email, username y nombre) o hace `reset()` en Amplitude cuando se cierra sesion.

## Como enviar eventos adicionales

Usa el helper `trackEvent` en cualquier componente o store:

```ts
import { trackEvent } from '../lib/analytics'

void trackEvent('message_sent', {
  messageId,
  hasAttachments: files.length > 0,
})
```

Si necesitas asociar un usuario manualmente (casos especiales como bots), tambien puedes usar `setAnalyticsUser`/`clearAnalyticsUser` del mismo modulo.

