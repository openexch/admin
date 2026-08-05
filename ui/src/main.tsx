// SPDX-License-Identifier: Apache-2.0
import ReactDOM from 'react-dom/client'

// Self-hosted fonts (no runtime Google Fonts request). The theme maps
// --font-display to Space Grotesk and --font-sans to Inter (see index.css).
import '@fontsource/inter/400.css'
import '@fontsource/inter/500.css'
import '@fontsource/inter/600.css'
import '@fontsource/inter/700.css'
import '@fontsource/space-grotesk/500.css'
import '@fontsource/space-grotesk/600.css'
import '@fontsource/space-grotesk/700.css'

import './index.css'
import { AdminPage } from './pages/AdminPage'

// The admin console is the whole app here. AdminPage wraps itself in its
// ToastProvider; useTheme (inside it) owns the light/dark class after the
// inline no-flash script in index.html has set it pre-paint.
ReactDOM.createRoot(document.getElementById('root')!).render(<AdminPage />)
