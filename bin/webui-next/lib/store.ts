import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface SettingsState {
  darkMode: boolean
  autoRefresh: boolean
  refreshInterval: number
  toggleDarkMode: () => void
  toggleAutoRefresh: () => void
  setRefreshInterval: (interval: number) => void
}

export const useSettings = create<SettingsState>()(
  persist(
    (set) => ({
      darkMode: false,
      autoRefresh: true,
      refreshInterval: 5000,
      toggleDarkMode: () => set((state) => ({ darkMode: !state.darkMode })),
      toggleAutoRefresh: () => set((state) => ({ autoRefresh: !state.autoRefresh })),
      setRefreshInterval: (interval) => set({ refreshInterval: interval }),
    }),
    {
      name: 'bicycle-settings',
    }
  )
)
