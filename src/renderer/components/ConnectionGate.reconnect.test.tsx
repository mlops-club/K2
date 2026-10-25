// @vitest-environment jsdom
// A remote server goes away and comes back (a k2-kind-dist roll: both pods
// restart, same volumes, so the app's session token stays valid). Through
// the REAL ConnectionGate soft-poll loop: once /boot-status answers ready
// again, recovery must return to 'connected' by itself, with no host switch
// and no sign-in.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const h = vi.hoisted(() => {
  globalThis.fetch = (async () => new Response('{}', { status: 200 })) as typeof fetch
  return {
    // hang: requests time out (their abort signal fires); stuck: requests
    // never settle at all, abort or not (a wedged webview socket).
    server: { up: true, instanceId: 'boot-1', hang: false, stuck: false },
    bootPolls: 0,
    arbiterSawReady: 0,
    app: { mounts: 0 },
  }
})

vi.mock('@tauri-apps/api/core', () => ({
  invoke: vi.fn(async (cmd: string) => {
    if (cmd === 'daemon_ws_url') return { state: 'available', port: 4100, token: 'own' }
    if (cmd === 'remote_boot_probe') {
      // The arbiter's fresh socket goes through the same proxy: refused
      // while down, its own 4 s timeout while the pod behind it is gone.
      if (!h.server.up || h.server.hang) throw new Error('connection refused')
      h.arbiterSawReady += 1
      return { status: 200, body: JSON.stringify({ phase: 'ready' }) }
    }
    return null
  }),
}))
vi.mock('@tauri-apps/api/app', () => ({ getVersion: async () => '9.9.9' }))
vi.mock('@tauri-apps/api/event', () => ({
  emit: vi.fn(async () => undefined),
  listen: vi.fn(async () => () => undefined),
}))
vi.mock('@tauri-apps/api/window', () => ({
  getCurrentWindow: () => ({ label: 'main' }),
}))
vi.mock('@/components/TopBar/GateChrome', () => ({ default: () => null }))
vi.mock('@/App', async () => {
  const React = await import('react')
  function App(): React.JSX.Element {
    React.useEffect(() => {
      h.app.mounts += 1
    }, [])
    return <div data-stub-app="" />
  }
  return { default: App, HomeRoomsHost: () => null, ZenHost: () => null }
})

import { act, cleanup, render } from '@testing-library/react'
import { ConnectionGate } from './ConnectionGate'
import { useConnectHostStore, type ConnectHost } from '@/stores/connect-host'
import { noteRemoteEventsClosed, resetRemoteEventsDropState } from '@/lib/remote-ws-drop'
import { __resetForceProbeLatchForTests } from '@/lib/connection-gate-probe'

const KIND: ConnectHost = {
  id: 'id-kind', label: 'kind dist', hostname: '127.0.0.1', port: 7443, secure: false,
  token: 'session-tok', remember: false, lastConnectedAt: null,
} as ConnectHost

function bootBody(): string {
  return JSON.stringify({
    view: 'full', phase: 'ready', protocol: 1, protocolMin: 1,
    instanceId: h.server.instanceId, version: '0.46.3', features: [],
  })
}

/** The server as the webview sees it: refused while down, a fetch that
 *  never answers (until its abort signal fires) while `hang`, and one that
 *  never settles at all while `stuck`. */
async function fakeFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const url = String(input)
  if (url.includes('/boot-status')) h.bootPolls += 1
  if (h.server.stuck) return new Promise(() => undefined)
  if (h.server.hang) {
    return new Promise((_, reject) => {
      init?.signal?.addEventListener('abort', () => reject(init.signal?.reason ?? new Error('aborted')))
    })
  }
  if (!h.server.up) throw new TypeError('Load failed')
  if (url.includes('/boot-status')) return new Response(bootBody(), { status: 200 })
  if (url.includes('/cli/auth/whoami')) return new Response('{"username":"owner"}', { status: 200 })
  return new Response('{}', { status: 200 })
}

/** One turn of the REAL event loop (setImmediate is not faked here; the
 *  renderer's lib types don't declare it). */
function realMacrotask(): Promise<void> {
  const g = globalThis as unknown as { setImmediate: (cb: () => void) => void }
  return new Promise((resolve) => g.setImmediate(resolve))
}

/** Advance fake time in small steps so every awaited fetch / timer runs.
 *  Each step also yields to the real event loop: the gate's dynamic
 *  imports resolve in real time, and must not fall behind fake time. */
async function advance(ms: number, step = 250): Promise<void> {
  for (let t = 0; t < ms; t += step) {
    await act(async () => {
      await vi.advanceTimersByTimeAsync(step)
      await realMacrotask()
    })
  }
}

function setVisibility(state: 'visible' | 'hidden'): void {
  Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => state })
}

const recovery = (): string => useConnectHostStore.getState().recovery.kind

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date'] })
  h.server.up = true
  h.server.hang = false
  h.server.stuck = false
  h.server.instanceId = 'boot-1'
  setVisibility('visible')
  h.bootPolls = 0
  h.arbiterSawReady = 0
  h.app.mounts = 0
  vi.stubGlobal('fetch', fakeFetch)
  useConnectHostStore.setState({
    hosts: [KIND],
    activeHost: KIND,
    recovery: { kind: 'connected' },
    hydrateFromDisk: async () => undefined,
  } as never)
  resetRemoteEventsDropState()
  __resetForceProbeLatchForTests()
  sessionStorage.clear()
})

afterEach(() => {
  cleanup()
  vi.useRealTimers()
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

async function connect(): Promise<void> {
  render(<ConnectionGate />)
  await advance(3_000)
  expect(h.app.mounts).toBe(1)
  expect(recovery()).toBe('connected')
}

describe('ConnectionGate: a remote that goes away and comes back reconnects by itself', () => {
  // The first ok poll after a >60 s outage used to consult the arbiter
  // whenever its 30 s throttle had run out, i.e. on one poll in four. With
  // the jitter fixed (polls every ~8 s) these outage lengths, one poll
  // apart, put the server's return on every phase of that cycle, so one of
  // them always lands on the arbiter's poll.
  it.each([63_000, 71_000, 79_000, 87_000, 95_000])(
    'server refused for %i ms, back with a new instanceId → connected, never treated as a wedge',
    async (outageMs) => {
      vi.spyOn(Math, 'random').mockReturnValue(0.999)
      await connect()

      // The roll: the events WS drops (R1) and every request is refused.
      h.server.up = false
      act(() => noteRemoteEventsClosed())
      await advance(outageMs)
      expect(recovery()).toBe('reconnecting')

      const pollsWhileDown = h.bootPolls
      h.server.up = true
      h.server.instanceId = 'boot-2'
      // RECOVERY_RECONNECT_MAX_MS (8 s), then a margin.
      await advance(12_000)
      // The webview's own probe answered, so the out-of-webview arbiter is
      // not asked. Before the fix it was: it said 'ready', the gate took
      // the webview's pool for wedged, called location.reload() (a no-op
      // here, as when a reload doesn't take) and stopped polling.
      expect(h.arbiterSawReady).toBe(0)
      expect(recovery()).toBe('connected')
      // ... and the loop is still alive afterwards: the health poll goes on.
      const pollsAtRecovery = h.bootPolls
      expect(pollsAtRecovery).toBeGreaterThan(pollsWhileDown)
      await advance(30_000)
      expect(h.bootPolls).toBeGreaterThan(pollsAtRecovery)
      expect(h.app.mounts).toBe(1) // stayed mounted: no sign-in, no remount
    },
  )

  it('requests that hang (proxy up, pod gone) then a refused gap, then back → connected', async () => {
    await connect()

    // socat accepts the connection but the pod behind it is gone: every
    // fetch hangs until its own timeout fires.
    h.server.hang = true
    act(() => noteRemoteEventsClosed())
    await advance(30_000)
    h.server.hang = false
    h.server.up = false
    await advance(20_000)
    expect(recovery()).toBe('reconnecting')

    h.server.up = true
    h.server.instanceId = 'boot-2'
    await advance(12_000)
    expect(recovery()).toBe('connected')
  })

  it('a /boot-status request that never settles (abort ignored) does not stall the loop', async () => {
    await connect()

    h.server.stuck = true
    act(() => noteRemoteEventsClosed())
    await advance(20_000)
    expect(recovery()).toBe('reconnecting')

    h.server.stuck = false
    h.server.instanceId = 'boot-2'
    // The tick watchdog (45 s) abandons the stuck tick, then one backoff.
    await advance(60_000)
    expect(recovery()).toBe('connected')
    expect(h.app.mounts).toBe(1)
  })

  it('a window hidden through the whole outage still reconnects', async () => {
    await connect()

    setVisibility('hidden')
    h.server.up = false
    act(() => noteRemoteEventsClosed())
    await advance(30_000)
    expect(recovery()).toBe('reconnecting')

    h.server.up = true
    h.server.instanceId = 'boot-2'
    await advance(12_000)
    expect(recovery()).toBe('connected')
  })
})
