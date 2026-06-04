import type { NamespaceInfo, EntityOverview, MessageInfo, EmulatorInfo } from '../types'

const BASE = '/api/dashboard'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  return res.json()
}

async function del(path: string): Promise<void> {
  const res = await fetch(`${BASE}${path}`, { method: 'DELETE' })
  if (!res.ok) throw new Error(`API error: ${res.status}`)
}

export const api = {
  getInfo: () => get<EmulatorInfo>('/info'),

  getNamespaces: () => get<NamespaceInfo[]>('/namespaces'),

  getEntities: (ns: string) => get<EntityOverview>(`/namespaces/${ns}/entities`),

  getQueueMessages: (ns: string, queueName: string) =>
    get<MessageInfo[]>(`/namespaces/${ns}/queues/${queueName}/messages`),

  getTopicMessages: (ns: string, topicName: string) =>
    get<MessageInfo[]>(`/namespaces/${ns}/topics/${topicName}/messages`),

  getDeadLetterMessages: (ns: string, queueName: string) =>
    get<MessageInfo[]>(`/namespaces/${ns}/queues/${queueName}/deadletter`),

  purgeQueue: (ns: string, queueName: string) =>
    del(`/namespaces/${ns}/queues/${queueName}/messages`),

  purgeDeadLetter: (ns: string, queueName: string) =>
    del(`/namespaces/${ns}/queues/${queueName}/deadletter`),
}
